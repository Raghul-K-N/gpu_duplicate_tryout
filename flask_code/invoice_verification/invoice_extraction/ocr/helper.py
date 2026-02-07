from invoice_verification.logger.logger import log_message
from typing import List
import numpy as np

LATIN_LANGS = [
            "af",
            "az",
            "bs",
            "cs",
            "cy",
            "da",
            "de",
            "es",
            "et",
            "fr",
            "ga",
            "hr",
            "hu",
            "id",
            "is",
            "it",
            "ku",
            "la",
            "lt",
            "lv",
            "mi",
            "ms",
            "mt",
            "nl",
            "no",
            "oc",
            "pi",
            "pl",
            "pt",
            "ro",
            "rs_latin",
            "sk",
            "sl",
            "sq",
            "sv",
            "sw",
            "tl",
            "tr",
            "uz",
            "vi",
            "french",
            "german",
            "fi",
            "eu",
            "gl",
            "lb",
            "rm",
            "ca",
            "qu",
        ]

def merge_text_with_spaces(results, 
                        confidence_threshold: float=0.8
                        ) -> List:
    """
    Merge text segments with proper spacing based on bounding box positions
    
    Args:
        results: PaddleOCR results containing text and bounding boxes
        confidence_threshold: Minimum confidence score to include text (default: 0.8)
    
    Returns:
        list: List of merged text lines with proper spacing
    """
    try:
        if not results:
            return []
        
        log_message(f"\nApplying confidence threshold of {confidence_threshold}")
        log_message("-" * 50)
        
        global_x_left = 99999

        all_merged_lines = []
        
        for page_num, sublist in enumerate(results, start=1):
            
            if not sublist:
                # all_merged_lines.append(f"{'='*34}<Page {page_num} - END>{'='*32}")
                continue
            
            # Add page separator to the output
            all_merged_lines.append(f"{'='*34}<Page {page_num} - START>{'='*32}")
            # Group text segments by vertical position (y-coordinate)
            lines = {}
            
            for each in sublist:
                box = each[0]
                text = each[1][0]
                confidence = each[1][1]
                
                if confidence < confidence_threshold:
                    # log_message(f"Skipping low confidence text: '{text}' (confidence: {confidence:.2f})")
                    continue
                
                # TODO: Consider vectorizing the y-center calculation if processing many segments.
                top_left , top_right, bottom_right, bottom_left = box
                max_top = max(top_left[1], top_right[1])
                max_bottom = max(bottom_left[1], bottom_right[1])
                y_center = (max_top + max_bottom ) / 2
                # y_center = (np.mean([point[1] for point in box]))
                # log_message(f"Text: '{text}' (y-center: {y_center:.2f})")
                # Group segments that are on the same line (within 10 pixels)
                line_key = round(y_center / 10) * 10
                
                if line_key not in lines:
                    lines[line_key] = []
                lines[line_key].append({
                    'text': text,
                    'box': box,
                    'x_left': min(point[0] for point in box),
                    'x_right': max(point[0] for point in box),
                    'confidence': confidence
                })
                # Update global leftmost x-coordinate
                global_x_left = min(global_x_left, min(point[0] for point in box))
            
            merged_lines = []
            for y_pos in sorted(lines.keys()):
                line_segments = lines[y_pos]
                # Sort segments by x-coordinate
                line_segments.sort(key=lambda x: x['x_left'])
                
                merged_text = ""
                for i, segment in enumerate(line_segments):
                    text = segment['text']
                    if i ==0:
                        text_left  = segment['x_left']

                        gap = text_left - global_x_left
                        # log_message(f"gap: {gap} and text_left: {text_left} and global_x_left: {global_x_left}")
                        if gap > 10:  # Threshold for adding a space
                            no_of_spaces = round(gap / 10)
                            merged_text += " " * no_of_spaces
                            # log_message(f"len: {no_of_spaces} and merged_text: {merged_text}")
                        
                    if i > 0:
                        prev_segment = line_segments[i-1]
                        gap = segment['x_left'] - prev_segment['x_right']
                        if gap > 10:  # Threshold for adding a space
                            no_of_spaces = round(gap / 10)
                            merged_text += " " * no_of_spaces
                    # Special handling for colons and numbers
                    if text.startswith(':'):
                        merged_text = merged_text.rstrip() + text
                    elif text[0].isdigit() and merged_text and not merged_text.endswith(' '):
                        merged_text += " " + text
                    else:
                        merged_text += text
                if merged_text.strip():
                    # merged_lines.append(merged_text.strip())
                    merged_lines.append(merged_text)
            merged_lines.append(f"{'='*34}<Page {page_num} - END>{'='*32}")
            all_merged_lines.extend(merged_lines)
            
            log_message(f"\nTotal text segments processed: {len(sublist)}")
            log_message(f"Text segments kept after filtering: {sum(len(lines[key]) for key in lines)}")
        
        log_message(f"\nTotal text segments processed: {len(results[0])}")
        log_message("-" * 50)
        return all_merged_lines
    except Exception as e:
        log_message(f"Error in merge_text_with_spaces: {e}", error_logger=True)
        return []
    
def adapt_paddle_result(pipeline_output):
    """
    Converts Paddle Structure V3 output (Flat Box) into the Legacy Format (Polygon Box).

    Input Box:  [xmin, ymin, xmax, ymax]
    Output Box: [[xmin, ymin], [xmax, ymin], [xmax, ymax], [xmin, ymax]]

    Returns: List of lists compatible with 'merge_text_with_spaces'
    """
    formatted_pages = []

    for page_data in pipeline_output:
        # 1. Safety Extraction
        # if 'res' not in page_data:
        #     continue

        res = page_data
        raw_boxes = res.get('rec_boxes', [])
        texts = res.get('rec_texts', [])
        scores = res.get('rec_scores', [])

        # 2. Normalize Numpy to List
        if isinstance(raw_boxes, np.ndarray):
            raw_boxes = raw_boxes.tolist()
        if isinstance(scores, np.ndarray):
            scores = scores.tolist()

        page_items = []

        for i in range(len(texts)):
            box = raw_boxes[i]
            text = texts[i]
            conf = scores[i]

            # 3. CRITICAL CONVERSION: Flat List -> 4-Point Polygon
            # Your function expects: top_left, top_right, bottom_right, bottom_left

            formatted_box = []

            # Check if input is [xmin, ymin, xmax, ymax] (length 4 flat list)
            if isinstance(box[0], (int, float)):
                x_min, y_min, x_max, y_max = box

                # Create the 4 corners manually
                top_left = [x_min, y_min]
                top_right = [x_max, y_min]
                bottom_right = [x_max, y_max]
                bottom_left = [x_min, y_max]

                formatted_box = [top_left, top_right, bottom_right, bottom_left]
            else:
                # If it's already a polygon [[x,y]...], use as is
                formatted_box = box

            # 4. Append in the exact structure: [box, [text, confidence]]
            page_items.append([formatted_box, [text, conf]])

        formatted_pages.append(page_items)

    return formatted_pages