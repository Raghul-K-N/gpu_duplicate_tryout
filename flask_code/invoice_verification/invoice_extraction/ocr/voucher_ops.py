from invoice_verification.logger.logger import log_message
import fitz  # PyMuPDF
import cv2
import numpy as np
from typing import List, Any, Dict, Tuple
from datetime import datetime

def get_image_and_scale(pdf_path: str, ocr_result: List[Any]) -> Tuple[Any, float, float]:
    """
    Renders PDF at 300 DPI and calculates accurate scaling from OCR coordinates.
    
    Args:
        pdf_path: Path to PDF file
        ocr_result: PaddleOCR result containing bboxes
        ocr_image_path: Path to the actual image used for OCR (if different from rendered PDF)
    """
    try:
        doc = fitz.open(pdf_path)
        page = doc[0]
        
        # Render High-Res Image (300 DPI) for visualization
        pix = page.get_pixmap(dpi=300)
        img_h, img_w = pix.h, pix.w
        
        # Fast numpy conversion
        img_array = np.frombuffer(pix.samples, dtype=np.uint8).reshape(img_h, img_w, pix.n)
        if img_array.shape[2] == 4:
            img_array = cv2.cvtColor(img_array, cv2.COLOR_RGBA2RGB)

        # --- PROPER SCALE CALCULATION ---
        # Find the actual dimensions OCR was performed on
        all_xs = []
        all_ys = []
        for line in ocr_result[0]:
            bbox = line[0]
            all_xs.extend([bbox[0][0], bbox[1][0], bbox[2][0], bbox[3][0]])
            all_ys.extend([bbox[0][1], bbox[1][1], bbox[2][1], bbox[3][1]])
        
        if not all_xs or not all_ys:
            log_message("[ERROR] No OCR text found to calculate scale.")
            return img_array, 1.0, 1.0

        # Get OCR coordinate space dimensions
        ocr_max_x = max(all_xs)
        ocr_max_y = max(all_ys)
        
        # Calculate scale factors
        scale_x = img_w / ocr_max_x
        scale_y = img_h / ocr_max_y
        
        log_message(f"[INIT] Rendered Image: {img_w}x{img_h}")
        log_message(f"[INIT] OCR Coordinate Space: {ocr_max_x:.1f}x{ocr_max_y:.1f}")
        log_message(f"[INIT] Scale Factors: X={scale_x:.4f}, Y={scale_y:.4f}")
        
        # Validate scale factors
        if abs(scale_x - scale_y) > 0.1:
            log_message(f"[WARNING] Non-uniform scaling detected! X={scale_x:.4f}, Y={scale_y:.4f}")
        
        return img_array, scale_x, scale_y

    except Exception as e:
        log_message(f"[ERROR] PDF Processing: {e}")
        return None, 1.0, 1.0

def analyze_mark_center_density(crop_img, label: str) -> float:
    """
    Finds the checkbox/radio contour and measures the ink density ONLY in its center.
    This prevents thick borders from registering as 'filled'.
    """
    if crop_img.size == 0:
        return 0.0

    # 1. Preprocess
    gray = cv2.cvtColor(crop_img, cv2.COLOR_RGB2GRAY)
    # Otsu thresholding adapts to lighting/scanning noise
    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    # 2. Find Contours
    contours, _ = cv2.findContours(binary, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
    
    img_h, img_w = binary.shape
    best_score = 0.0
    candidate_count = 0

    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        
        # --- FILTERING ---
        # Ignore Noise (<10px) or Huge Text (>60px)
        if w < 10 or h < 10: continue
        if w > 60 or h > 60: continue
        
        # Ignore Rectangles (must be square-ish/round 0.6-1.5)
        aspect = w / float(h)
        if aspect < 0.6 or aspect > 1.5: continue
        
        # Ignore Right-side noise (Text starts at right of crop)
        # We only look in the LEFT 75% of the crop
        if x > (img_w * 0.75): continue

        candidate_count += 1
        
        # --- CENTER MASS ANALYSIS ---
        # Extract the "Bullseye" (Middle 40% of the box)
        center_x = x + int(w * 0.30)
        center_y = y + int(h * 0.30)
        center_w = int(w * 0.40)
        center_h = int(h * 0.40)
        
        # Safety bounds
        if center_w <= 0 or center_h <= 0: continue

        roi_center = binary[center_y:center_y+center_h, center_x:center_x+center_w]
        
        if roi_center.size > 0:
            # Calculate fill density of the CENTER only
            density = np.count_nonzero(roi_center) / roi_center.size
            if density > best_score:
                best_score = density

    if candidate_count == 0:
        # Fallback: No clean box found (maybe a free-floating X)
        # Check raw density of the left half
        left_half = binary[:, :int(img_w * 0.6)]
        raw_density = np.count_nonzero(left_half) / left_half.size
        # Raw density needs a lower threshold to trigger
        return raw_density * 1.5 

    return best_score

def check_field_debug(img_array, scale_x, scale_y, ocr_map, target_text, debug_visuals: List):
    # 1. Lookup
    target_lower = target_text.lower()
    target_bbox = None
    matched_key = None
    
    # Exact match first
    if target_lower in ocr_map:
        target_bbox = ocr_map[target_lower]
        matched_key = target_lower
    else:
        # Partial match
        for key, bbox in ocr_map.items():
            if target_lower in key:
                target_bbox = bbox
                matched_key = key
                break
    
    if not target_bbox:
        log_message(f"[MISSING] '{target_text}' not in OCR.")
        return False, 0.0

    # 2. Apply SEPARATE scale factors for X and Y coordinates
    xs = [p[0] for p in target_bbox]
    ys = [p[1] for p in target_bbox]
    
    # Scale X coordinates
    x1 = int(min(xs) * scale_x)
    x2 = int(max(xs) * scale_x)
    
    # Scale Y coordinates  
    y1 = int(min(ys) * scale_y)
    y2 = int(max(ys) * scale_y)
    
    log_message(f"   [COORDS] '{target_text}' -> OCR: ({min(xs):.1f},{min(ys):.1f}) -> Scaled: ({x1},{y1})")

    # 3. OPTIMIZED CROP STRATEGY
    if "no invoice" in target_lower:
        # No Invoice: Checkbox is directly to the left
        crop_x1 = max(0, x1 - 45)  # Shift more right (was 55)
        crop_x2 = min(img_array.shape[1], x1 + 30)  # Extended right edge
        # Move up significantly to align with checkbox row
        crop_y1 = max(0, y1 - 58)  # Move up more (was 52)
        crop_y2 = min(img_array.shape[0], y1 + 5)  # Reduced bottom
        
    elif any(pm in target_lower for pm in ["bank transfer", "check", "manual payment", "wire payment", "individual payment"]):
        # Payment Methods: Checkboxes are to the left of text
        crop_x1 = max(0, x1 - 15)  # Shift more right (was 25)
        crop_x2 = min(img_array.shape[1], x1 + 35)  # Extended right edge
        # Move up to align with checkbox center
        crop_y1 = max(0, y1 - 38)  # Keep same vertical (was 38)
        crop_y2 = min(img_array.shape[0], y1 + 15)  # Reduced bottom
        
    else:
        # Transaction Type (Invoice/Credit Note): Already working well
        crop_x1 = max(0, x1 - 70)
        crop_x2 = min(img_array.shape[1], x1 + 30)
        crop_y1 = max(0, y1 - 10)
        crop_y2 = min(img_array.shape[0], y2 + 10)

    crop_img = img_array[crop_y1:crop_y2, crop_x1:crop_x2]
    
    # 4. Analyze
    score = analyze_mark_center_density(crop_img, target_text)
    
    # 5. ADAPTIVE THRESHOLDS
    # Different checkbox styles require different thresholds
    # Solid filled circles/boxes: center density > 0.45
    # X marks or checkmarks: center density > 0.18
    # Lighter marks or scanned docs: > 0.15
    
    if "no invoice" in target_lower or any(pm in target_lower for pm in ["bank transfer", "check", "manual payment", "wire payment", "individual payment"]):
        # These forms often have lighter marks or X marks
        threshold = 0.18
    else:
        # Transaction types typically have solid filled circles
        threshold = 0.22
    
    is_checked = score > threshold
    
    status = "CHECKED" if is_checked else "..."
    log_message(f"   >>> {target_text}: {status} (Score: {score:.3f}, Threshold: {threshold})")
    
    # --- SAVE DEBUG INFO FOR VISUALIZATION ---
    debug_visuals.append({
        "label": target_text,
        "box": (crop_x1, crop_y1, crop_x2, crop_y2),
        "text_box": (x1, y1, x2, y2),  # Also save the text box location
        "score": score,
        "checked": is_checked
    })
    
    return is_checked, score

def save_debug_image(img_array, debug_visuals):
    """Draws bounding boxes and scores on the image and saves it."""
    # Convert RGB to BGR for OpenCV
    debug_img = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
    
    for item in debug_visuals:
        x1, y1, x2, y2 = item["box"]
        tx1, ty1, tx2, ty2 = item["text_box"]
        score = item["score"]
        checked = item["checked"]
        label = item["label"]
        
        # Color: Green if checked, Red if unchecked
        color = (0, 255, 0) if checked else (0, 0, 255)
        
        # Draw Search Area (crop region)
        cv2.rectangle(debug_img, (x1, y1), (x2, y2), color, 2)
        
        # Draw Text Location (blue dashed)
        cv2.rectangle(debug_img, (tx1, ty1), (tx2, ty2), (255, 0, 0), 1)
        
        # Draw Label + Score
        text = f"{label} ({score:.2f})"
        # Put text slightly above the box
        cv2.putText(debug_img, text, (x1, max(20, y1 - 10)), 
                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)

    # Generate Timestamped Filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"debug_view_{timestamp}.jpg"
    
    cv2.imwrite(filename, debug_img)
    log_message(f"\n[VISUAL] Debug image saved to: {filename}")

def extract_form_data(pdf_path: str, ocr_result: List[Any]) -> Dict[str, Any]:
    log_message("\n--- Starting Robust Extraction ---")
    img_array, scale_x, scale_y = get_image_and_scale(pdf_path, ocr_result)
    
    if img_array is None: return {}

    # Map OCR text to bbox for fast lookup
    ocr_map = {}
    for line in ocr_result[0]:
        t = line[1][0].strip().lower()
        ocr_map[t] = line[0]

    result_data = {
        "voucher_no_invoice": False,
        "payment_method": None,
        "transaction_type": None
    }
    
    # List to hold visualization data
    debug_visuals = []

    # 1. No Invoice
    log_message("\n--- Checking 'No Invoice' ---")
    checked, _ = check_field_debug(img_array, scale_x, scale_y, ocr_map, "No Invoice", debug_visuals)
    result_data["voucher_no_invoice"] = checked

    # 2. Payment Method
    log_message("\n--- Checking Payment Methods ---")
    methods = ["Bank Transfer", "Check", "Manual Payment", "Wire Payment", "Individual Payment"]
    best_method = None
    max_score = 0.0
    
    for method in methods:
        checked, score = check_field_debug(img_array, scale_x, scale_y, ocr_map, method, debug_visuals)
        if score > max_score and score > 0.20:
            max_score = score
            best_method = method
            
    result_data["payment_method"] = best_method

    # 3. Transaction Type
    log_message("\n--- Checking Transaction Type ---")
    _, inv_score = check_field_debug(img_array, scale_x, scale_y, ocr_map, "Invoice", debug_visuals)
    _, cre_score = check_field_debug(img_array, scale_x, scale_y, ocr_map, "Credit Note", debug_visuals)
    
    log_message(f"   [COMPARE] Invoice Score: {inv_score:.4f} vs Credit Score: {cre_score:.4f}")
    
    if inv_score > 0.20 and inv_score > cre_score:
        result_data["transaction_type"] = "debit"
    elif cre_score > 0.20 and cre_score > inv_score:
        result_data["transaction_type"] = "credit"
    else:
        result_data["transaction_type"] = None

    log_message(f"\n--- Final Result: {result_data} ---")
    
    # --- GENERATE VISUALIZATION ---
    # save_debug_image(img_array, debug_visuals)
    
    return result_data