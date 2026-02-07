"""
Mermaid to SVG Converter - Perfect Quality at Any Zoom
SVG is vector-based, so it never pixelates
"""

import sys
import os
import requests
import base64

def mermaid_to_svg(mermaid_code, output_file):
    """Convert Mermaid code to SVG using mermaid.ink API"""
    
    # Encode mermaid code
    graphbytes = mermaid_code.encode("utf8")
    base64_bytes = base64.b64encode(graphbytes)
    base64_string = base64_bytes.decode("ascii")
    
    # Use mermaid.ink SVG endpoint
    url = f"https://mermaid.ink/svg/{base64_string}"
    
    print(f"🔄 Converting Mermaid diagram to SVG (vector format)...")
    print(f"   Output: {output_file}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        with open(output_file, 'wb') as f:
            f.write(response.content)
        
        file_size_kb = len(response.content) / 1024
        print(f"✅ Success! File size: {file_size_kb:.1f} KB")
        print(f"   ✨ SVG format = Perfect quality at ANY zoom level!")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Error: {e}")
        return False

def read_mermaid_file(file_path):
    """Read mermaid code from file"""
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Remove markdown code blocks if present
    if '```mermaid' in content:
        start = content.find('```mermaid') + len('```mermaid')
        end = content.find('```', start)
        content = content[start:end].strip()
    elif '```' in content:
        start = content.find('```') + 3
        end = content.find('```', start)
        content = content[start:end].strip()
    
    return content

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python mermaid_to_svg.py <input.mmd> [output.svg]")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    # Auto-generate output filename
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    else:
        base = os.path.splitext(input_file)[0]
        output_file = f"{base}.svg"
    
    if not os.path.exists(input_file):
        print(f"❌ Error: Input file '{input_file}' not found")
        sys.exit(1)
    
    print(f"📖 Reading: {input_file}")
    mermaid_code = read_mermaid_file(input_file)
    
    success = mermaid_to_svg(mermaid_code, output_file)
    
    if success:
        print(f"\n✅ Complete!")
        print(f"   📂 File: {os.path.abspath(output_file)}")
        print(f"   🔍 Open in browser or insert into documents - scales perfectly!")
    else:
        sys.exit(1)
