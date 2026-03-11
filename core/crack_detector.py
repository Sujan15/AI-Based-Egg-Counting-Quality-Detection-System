# core/crack_detector.py

import cv2
import numpy as np
import openvino as ov

class CrackDetector:
    def __init__(self, model_path):
        self.core = ov.Core()
        # Load the segmentation model
        compiled_model = self.core.compile_model(model_path, "CPU")
        self.infer_request = compiled_model.create_infer_request()
        
        # Get input and output info
        self.input_layer = compiled_model.input(0)
        self.output_layers = compiled_model.outputs  # might be multiple
        
        # Model input dimensions
        self.input_h = self.input_layer.shape[2]
        self.input_w = self.input_layer.shape[3]

    def is_defective(self, crop):
        if crop is None or crop.size < 100:
            return False

        try:
            # Preprocess
            input_img = cv2.resize(crop, (self.input_w, self.input_h))
            input_blob = input_img.transpose(2, 0, 1)[np.newaxis, ...].astype(np.float32) / 255.0
            
            # Inference
            results = self.infer_request.infer({0: input_blob})
            
            # For YOLOv11-seg, output[0] is usually [1, 116, 8400] for detection,
            # and there may be a second output for masks. 
            # We'll use the detection part: shape (1, 116, 8400) where 116 = 4 (bbox) + 80 (classes) + 32 (masks)
            # We want the class confidence for the crack class (assume class 1).
            # If you only have one class (egg) and crack is a property, you might need a different approach.
            # For now, we assume crack is class 1.
            
            det_output = list(results.values())[0]  # first output
            det_output = np.squeeze(det_output)      # (116, 8400)
            
            # Transpose if needed (usually (116, 8400) but could be (8400, 116))
            if det_output.shape[0] == 8400:
                det_output = det_output.T
            
            # Extract class scores for class 1 (crack)
            class_scores = det_output[5:, :]   # skip first 5 (bbox)
            crack_scores = class_scores[1, :]   # assuming class 1 is crack
            
            max_crack_conf = np.max(crack_scores)
            
            # Threshold – adjust based on your model's performance (try 0.5)
            if max_crack_conf > 0.90:
                return True
                
            return False

        except Exception as e:
            print(f"Crack Detection Error: {e}")
            return False
