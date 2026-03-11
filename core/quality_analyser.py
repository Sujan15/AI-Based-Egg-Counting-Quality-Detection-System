# core/quality_analyser.py

import cv2
import numpy as np
import openvino as ov

class QualityAnalyser:
    def __init__(self, model_path):
        core = ov.Core()
        self.model = core.compile_model(model_path, "CPU")
        self.output_layer = self.model.output(0)

    def is_defective(self, crop):
        if crop is None or crop.size < 100: return False
        
        # Prep crop for YOLOv11-Seg
        blob = cv2.resize(crop, (160, 160)) # Smaller for speed
        blob = blob.transpose(2,0,1)[np.newaxis,...]
        
        results = self.model([blob])[self.output_layer]
        
        # If defect segment mask area > threshold
        if np.max(results) > 0.6: # Simple confidence threshold
            return True
        return False