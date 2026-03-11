# core/tracker.py
import numpy as np
import supervision as sv

class KalmanFilter1D:
    """
    Simple 1D constant‑velocity Kalman filter for vertical position.
    State: [y, vy]
    """
    def __init__(self, init_y):
        self.x = np.array([init_y, 0.0], dtype=float)
        self.P = np.eye(2) * 10.0
        self.F = np.array([[1, 1], [0, 1]], dtype=float)   # dt = 1 frame
        self.H = np.array([[1, 0]], dtype=float)
        self.R = 5.0      # measurement noise
        self.Q = np.eye(2) * 0.1   # process noise

    def predict(self):
        self.x = self.F @ self.x
        self.P = self.F @ self.P @ self.F.T + self.Q

    def update(self, z):
        y = z - (self.H @ self.x).item()
        S = (self.H @ self.P @ self.H.T).item() + self.R
        K = (self.P @ self.H.T) / S
        self.x = self.x + K.flatten() * y
        self.P = (np.eye(2) - K @ self.H) @ self.P


class Track:
    """Internal track representation."""
    def __init__(self, bbox, track_id):
        self.track_id = track_id
        self.last_bbox = bbox          # last measured bbox (for IoU)
        self.age = 0                   # frames since last hit
        self.hit_streak = 1
        cy = (bbox[1] + bbox[3]) / 2.0
        self.kf = KalmanFilter1D(cy)

    def predict(self):
        self.kf.predict()

    def update(self, bbox):
        cy = (bbox[1] + bbox[3]) / 2.0
        self.kf.update(cy)
        self.last_bbox = bbox
        self.age = 0
        self.hit_streak += 1


class EggTracker:
    """
    Industrial conveyor tracker with motion constraints and Kalman prediction.
    """
    def __init__(self, max_lost=60, max_speed_px=15, min_iou=0.3):
        """
        Args:
            max_lost: frames to keep a track without detection
            max_speed_px: maximum vertical displacement (pixels) per frame
            min_iou: minimum IoU to consider a match
        """
        self.tracks = {}          # track_id -> Track
        self.next_id = 0
        self.max_lost = max_lost
        self.max_speed_px = max_speed_px
        self.min_iou = min_iou

    def update(self, detections: sv.Detections) -> sv.Detections:
        """
        Assign tracker IDs to detections, enforcing conveyor physics.
        Returns a new Detections object with `tracker_id` field.
        """
        if len(detections) == 0:
            self._predict_all()
            self._remove_old_tracks()
            empty = sv.Detections.empty()
            empty.tracker_id = np.array([], dtype=int)
            return empty

        # 1. Predict all tracks to current frame
        self._predict_all()

        # 2. Match detections to existing tracks
        matches, unmatched_det = self._match(detections)

        # 3. Prepare tracker_id array (default -1 for unmatched)
        tracker_ids = np.full(len(detections), -1, dtype=int)

        # 4. Update matched tracks and assign IDs
        for track_id, det_idx in matches.items():
            track = self.tracks[track_id]
            track.update(detections.xyxy[det_idx])
            tracker_ids[det_idx] = track_id

        # 5. Create new tracks for unmatched detections
        for det_idx in unmatched_det:
            track_id = self._create_track(detections.xyxy[det_idx])
            tracker_ids[det_idx] = track_id

        # 6. Increase age for tracks that were not matched
        matched_track_ids = set(matches.keys())
        for tid, track in self.tracks.items():
            if tid not in matched_track_ids:
                track.age += 1

        # 7. Remove tracks that have been lost for too long
        self._remove_old_tracks()

        # 8. Build new Detections object with tracker_id
        return sv.Detections(
            xyxy=detections.xyxy,
            confidence=detections.confidence,
            class_id=detections.class_id,
            tracker_id=tracker_ids,
            mask=detections.mask if detections.mask is not None else None
        )

    def _predict_all(self):
        for track in self.tracks.values():
            track.predict()

    def _match(self, detections):
        """
        Greedy matching with motion constraints and IoU.
        Returns (matches: dict{track_id: det_idx}, unmatched_det: set of indices)
        """
        num_tracks = len(self.tracks)
        num_dets = len(detections)
        if num_tracks == 0 or num_dets == 0:
            return {}, set(range(num_dets))

        # Pre‑compute IoU matrix, setting invalid pairs to -1
        track_ids = list(self.tracks.keys())
        iou_mat = np.zeros((num_tracks, num_dets))
        for i, tid in enumerate(track_ids):
            track = self.tracks[tid]
            for j in range(num_dets):
                bbox = detections.xyxy[j]
                if self._motion_allowed(track, bbox):
                    iou = self._iou(track.last_bbox, bbox)
                    iou_mat[i, j] = iou
                else:
                    iou_mat[i, j] = -1.0

        matches = {}
        used_tracks = set()
        used_dets = set()

        # Greedy assignment: for each detection, pick best allowed track
        for j in range(num_dets):
            best_iou = self.min_iou
            best_i = -1
            for i in range(num_tracks):
                if i in used_tracks:
                    continue
                if iou_mat[i, j] > best_iou:
                    best_iou = iou_mat[i, j]
                    best_i = i
            if best_i != -1:
                track_id = track_ids[best_i]
                matches[track_id] = j
                used_tracks.add(best_i)
                used_dets.add(j)

        unmatched_det = set(range(num_dets)) - used_dets
        return matches, unmatched_det

    def _motion_allowed(self, track, bbox):
        """Check if detection obeys forward motion and speed limits."""
        cy = (bbox[1] + bbox[3]) / 2.0
        last = track.last_bbox
        last_cy = (last[1] + last[3]) / 2.0

        # No backward motion (allow 2px tolerance for jitter)
        if cy < last_cy - 2.0:
            return False
        # Speed limit
        if cy - last_cy > self.max_speed_px:
            return False
        return True

    @staticmethod
    def _iou(bbox1, bbox2):
        x1 = max(bbox1[0], bbox2[0])
        y1 = max(bbox1[1], bbox2[1])
        x2 = min(bbox1[2], bbox2[2])
        y2 = min(bbox1[3], bbox2[3])
        inter = max(0, x2 - x1) * max(0, y2 - y1)
        area1 = (bbox1[2] - bbox1[0]) * (bbox1[3] - bbox1[1])
        area2 = (bbox2[2] - bbox2[0]) * (bbox2[3] - bbox2[1])
        union = area1 + area2 - inter
        return inter / union if union > 0 else 0.0

    def _create_track(self, bbox):
        track_id = self.next_id
        self.next_id += 1
        self.tracks[track_id] = Track(bbox, track_id)
        return track_id

    def _remove_old_tracks(self):
        to_remove = [tid for tid, trk in self.tracks.items() if trk.age > self.max_lost]
        for tid in to_remove:
            del self.tracks[tid]