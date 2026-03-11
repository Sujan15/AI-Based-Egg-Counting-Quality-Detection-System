# core/vision_engine.py

import cv2
import numpy as np
import openvino as ov
import supervision as sv
from core.tracker import EggTracker
from core.sizer import EggSizer
from core.crack_detector import CrackDetector
import time
from core.logger_setup import ai_logger, prod_logger, log_ai_perf, log_production_event

class EggVisionEngine:
    def __init__(self, config, line_config):
        self.core = ov.Core()

        self.last_log_time = time.time()
        self.frame_count = 0
        self.line_id = line_config['id']
        
        # 1. OpenVINO Compiled Model
        model_path = config['models']['detection']
        if model_path.endswith('.pt'):
            model_path = model_path.replace('.pt', '.xml')
            
        # Using "LATENCY" performance hint for real-time conveyor tracking
        compiled_model = self.core.compile_model(model_path, "CPU", {
            "PERFORMANCE_HINT": "LATENCY",
            "INFERENCE_NUM_THREADS": "4"
        })
        
        # Create a persistent infer request for this process
        self.infer_request = compiled_model.create_infer_request()
        
        # 2. Component Initialization
        self.tracker = EggTracker()
        self.sizer = EggSizer(line_config['ppm'])
        self.crack_detector = CrackDetector(config['models']['segmentation'])

        # 3. Conveyor Geometry
        self.line_y = line_config['camera']['counting_line_y']
        self.crack_zone = line_config['camera']['crack_zone'] 

        # 4. Industrial State Management (ID Locking)
        self.id_states = {} 
        self.counted_ids = set()
        self.stats = {"Small": 0, "Standard": 0, "Big": 0, "Cracked": 0}

    def process_frame(self, frame):
        start_time = time.perf_counter()
        if frame is None: return None, {}
        orig_h, orig_w = frame.shape[:2]

        # STEP 1: Preprocessing
        input_img = cv2.resize(frame, (640, 640))
        input_blob = input_img.transpose(2, 0, 1)[np.newaxis, ...].astype(np.float32) / 255.0

        # STEP 2: Inference (Standard API)
        self.infer_request.infer({0: input_blob})
        results = self.infer_request.get_output_tensor().data
        
        # STEP 3: YOLO Parsing & Tracking
        detections = self._parse_yolo(results, orig_w, orig_h)
        detections = self.tracker.update(detections)

        # STEP 4: Industrial ID Locking Logic
        if detections and detections.tracker_id is not None:
            for i in range(len(detections)):
                bbox = detections.xyxy[i]
                tid = detections.tracker_id[i]
                cx = int((bbox[0] + bbox[2]) / 2)
                cy = int((bbox[1] + bbox[3]) / 2)

                # Initialize State for new IDs
                if tid not in self.id_states:
                    self.id_states[tid] = {
                        "size": "Standard",
                        "is_cracked": False,
                        "locked": False,
                        "color": (0, 255, 0)
                    }

                state = self.id_states[tid]

                # --- STABLE ATTRIBUTE LOGIC ---
                if not state["locked"]:
                    # Update Size
                    size_label, color = self.sizer.calculate_size(bbox)
                    state["size"] = size_label
                    state["color"] = color

                    # Crack Detection (Only once per ID inside the zone)
                    zx1, zy1, zx2, zy2 = self.crack_zone
                    if zx1 < cx < zx2 and zy1 < cy < zy2:
                        crop = frame[max(0, int(bbox[1])):int(bbox[3]), 
                                     max(0, int(bbox[0])):int(bbox[2])]
                        
                        if self.crack_detector.is_defective(crop):
                            state["is_cracked"] = True
                            state["color"] = (0, 0, 255) 
                        
                        # LOCK ID: Lock classification once egg passes center of crack zone
                        if cy > (zy1 + zy2) / 2:
                            state["locked"] = True

                        # --- PRODUCTION LOGGING ---
                # Inside the counting logic block:
                if cy > self.line_y and tid not in self.counted_ids:
                    self.counted_ids.add(tid)
                    # LOG PRODUCTION EVENT IMMEDIATELY
                    log_production_event(self.line_id, tid, state["size"], state["is_cracked"])
                    
                    if state["is_cracked"]:
                        self.stats["Cracked"] += 1
                    else:
                        self.stats[state["size"]] += 1  

                # STEP 5: VISUALIZATION
                x1, y1, x2, y2 = map(int, bbox)
                cv2.rectangle(frame, (x1, y1), (x2, y2), state["color"], 2)
                
                status_text = "CRACKED" if state["is_cracked"] else state["size"]
                label = f"{status_text} ID:{tid}"
                cv2.putText(frame, label, (x1, y1-10), cv2.FONT_HERSHEY_SIMPLEX, 0.6, state["color"], 2)

        # STEP 6: Memory Cleanup
        if len(self.id_states) > 100:
            current_ids = detections.tracker_id if (detections and detections.tracker_id is not None) else []
            self.id_states = {k: v for k, v in self.id_states.items() if k in current_ids or k in self.counted_ids}

        self._draw_production_ui(frame)
        
        return frame, {
            "total": len(self.counted_ids), 
            "broken": self.stats["Cracked"], 
            "details": self.stats
        }

    def _parse_yolo(self, output, orig_w, orig_h):
        output = np.squeeze(output)
        if output.shape[0] < output.shape[1]:
            output = output.T

        scores = output[:, 4]
        mask = scores > 0.45
        valid_data = output[mask]
        valid_scores = scores[mask]

        if len(valid_data) == 0:
            return sv.Detections.empty()

        # Convert from centre‑size to xyxy
        x1 = (valid_data[:, 0] - valid_data[:, 2] / 2) * (orig_w / 640)
        y1 = (valid_data[:, 1] - valid_data[:, 3] / 2) * (orig_h / 640)
        x2 = (valid_data[:, 0] + valid_data[:, 2] / 2) * (orig_w / 640)
        y2 = (valid_data[:, 1] + valid_data[:, 3] / 2) * (orig_h / 640)

        xyxy = np.column_stack([x1, y1, x2, y2])

        # ---- ADD NMS ----
        # Sort by confidence descending
        indices = np.argsort(valid_scores)[::-1]
        keep = []
        while len(indices) > 0:
            i = indices[0]
            keep.append(i)
            # Compute IoU of box i with remaining boxes
            iou = self._batch_iou(xyxy[i], xyxy[indices[1:]])
            # Discard boxes with IoU > 0.5
            indices = indices[1:][iou <= 0.5]
        # ---- END NMS ----

        xyxy = xyxy[keep]
        conf = valid_scores[keep]
        class_id = np.zeros(len(keep), dtype=int)

        return sv.Detections(
            xyxy=xyxy,
            confidence=conf,
            class_id=class_id
        )

    def _batch_iou(self, box, boxes):
        """Compute IoU of a single box with an array of boxes."""
        x1 = np.maximum(box[0], boxes[:, 0])
        y1 = np.maximum(box[1], boxes[:, 1])
        x2 = np.minimum(box[2], boxes[:, 2])
        y2 = np.minimum(box[3], boxes[:, 3])
        inter = np.maximum(0, x2 - x1) * np.maximum(0, y2 - y1)
        area_box = (box[2] - box[0]) * (box[3] - box[1])
        area_boxes = (boxes[:, 2] - boxes[:, 0]) * (boxes[:, 3] - boxes[:, 1])
        union = area_box + area_boxes - inter
        return inter / union

    def _draw_production_ui(self, frame):
        h, w = frame.shape[:2]
        
        cv2.line(frame, (0, self.line_y), (w, self.line_y), (0, 255, 255), 2)
        cv2.putText(frame, "COUNTING LINE", (10, self.line_y - 10), 0, 0.5, (0, 255, 255), 1)

        zx1, zy1, zx2, zy2 = self.crack_zone
        cv2.rectangle(frame, (zx1, zy1), (zx2, zy2), (0, 0, 255), 1)
        cv2.putText(frame, "CRACK DETECTION ZONE", (zx1 + 5, zy1 + 20), 0, 0.5, (0, 0, 255), 1)

        overlay = frame.copy()
        cv2.rectangle(overlay, (5, 5), (230, 155), (0, 0, 0), -1)
        cv2.addWeighted(overlay, 0.7, frame, 0.3, 0, frame)
        
        total_count = len(self.counted_ids)
        cv2.putText(frame, f"TOTAL: {total_count}", (15, 30), 0, 0.7, (255, 255, 255), 2)
        cv2.putText(frame, f"Small: {self.stats['Small']}", (15, 60), 0, 0.6, (255, 255, 0), 1)
        cv2.putText(frame, f"Standard: {self.stats['Standard']}", (15, 85), 0, 0.6, (0, 255, 0), 1)
        cv2.putText(frame, f"Big: {self.stats['Big']}", (15, 110), 0, 0.6, (0, 165, 255), 1)
        cv2.putText(frame, f"Cracked: {self.stats['Cracked']}", (15, 135), 0, 0.6, (0, 0, 255), 1)