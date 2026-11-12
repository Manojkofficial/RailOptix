import os

pages_dir = os.path.join("dashboard", "pages")
files = [os.path.join(pages_dir, f) for f in os.listdir(pages_dir) if f.endswith(".py")]
files.append(os.path.join("dashboard", "app.py"))

count = 0
for fpath in files:
    with open(fpath, "r", encoding="utf-8") as f:
        content = f.read()
    new_content = content
    new_content = new_content.replace("use_container_width=True", 'width="stretch"')
    new_content = new_content.replace("use_container_width=False", 'width="content"')
    if new_content != content:
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(new_content)
        count += 1
        print(f"Fixed: {fpath}")

print(f"Total files fixed: {count}")
