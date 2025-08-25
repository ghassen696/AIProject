from ingest import find_all_html_files, is_content_page, extract_content
import json

ROOT_DIR = "C:\\Users\\g50047910\\Downloads\\Huawei Cloud Stack 8.3.1 Product Documentation 05\\Huawei Cloud Stack_8.3.1_05_en_YEN0426D\\resources"

html_files = find_all_html_files(ROOT_DIR)

documents = []
for file in html_files:
    if is_content_page(file):
        doc = extract_content(file)
        documents.append(doc)

# Save to JSON for next step
with open("html_docs.json", "w", encoding="utf-8") as f:
    json.dump(documents, f, indent=2, ensure_ascii=False)

print(f"✅ Saved {len(documents)} usable content pages.")
