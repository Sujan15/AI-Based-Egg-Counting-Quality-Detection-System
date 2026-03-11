# core/sizer.py

class EggSizer:
    def __init__(self, ppm):
        """
        ppm = pixels per millimeter (must be calibrated using real ruler on conveyor)
        """
        self.ppm = ppm

        # You should adjust these after measuring 20–30 real eggs
        self.small_threshold = 35   # mm
        self.big_threshold = 40     # mm

    def calculate_size(self, bbox):
        x1, y1, x2, y2 = bbox

        # Use both width and height for more stable sizing
        px_width = abs(x2 - x1)
        px_height = abs(y2 - y1)

        # Use larger dimension (egg may rotate)
        px_size = max(px_width, px_height)

        # Convert to millimeters
        mm_size = px_size / self.ppm

        # Safety clamp (avoid extreme jitter)
        if mm_size <= 0:
            return "Unknown", (128, 128, 128)

        # Classification
        if mm_size >= self.big_threshold:
            return "Big", (0, 165, 255)       # Orange

        elif mm_size >= self.small_threshold:
            return "Standard", (0, 255, 0)    # Green

        else:
            return "Small", (255, 255, 0)     # Cyan


