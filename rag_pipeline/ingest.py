# Step 1: Load and classify HTML files
from pathlib import Path
from bs4 import BeautifulSoup

def find_all_html_files(root_dir):
    return list(Path(root_dir).rglob("*.html"))

def is_content_page(filepath):
    try:
        soup = BeautifulSoup(open(filepath, "r", encoding="utf-8"), "html.parser")
        p_tags = soup.find_all("p")
        long_ps = [p.get_text(strip=True) for p in p_tags if len(p.get_text(strip=True)) > 100]
        return len(long_ps) >= 1
    except Exception as e:
        print(f"Failed to parse {filepath}: {e}")
        return False

def extract_content(filepath):
    soup = BeautifulSoup(open(filepath, "r", encoding="utf-8"), "html.parser")
    title = soup.title.string if soup.title else "No title"
    body = soup.get_text(separator=" ", strip=True)
    return {"filepath": str(filepath), "title": title, "content": body}
