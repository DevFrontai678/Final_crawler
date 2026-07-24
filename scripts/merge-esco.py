import csv
import json
import os

def extract_skills(csv_path):
    skills = []
    if not os.path.exists(csv_path):
        print(f"⚠️ File not found: {csv_path}")
        return skills
    
    # Force delimiter to comma (ESCO uses comma)
    delimiter = ','
    
    print(f"📂 Using delimiter: '{delimiter}' for {csv_path}")
    
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter=delimiter)
        header = reader.fieldnames
        print(f"📋 Header: {header}")
        
        # The correct column is 'preferredLabel'
        col = 'preferredLabel'
        if col not in header:
            print(f"⚠️ Column '{col}' not found. Available: {header}")
            return skills
        
        for row in reader:
            label = row.get(col, '').strip()
            if label and len(label) > 2:
                skills.append(label)
    
    return skills

en_file = 'src/ai/skills_en.csv'
de_file = 'src/ai/skills_de.csv'

# If the files don't exist, try to find them
if not os.path.exists(en_file) or not os.path.exists(de_file):
    import glob
    csv_files = glob.glob('src/ai/*.csv')
    for f in csv_files:
        if 'skills' in f.lower() and 'en' in f.lower():
            en_file = f
        if 'skills' in f.lower() and 'de' in f.lower():
            de_file = f

en_skills = extract_skills(en_file)
de_skills = extract_skills(de_file)

print(f"✅ English: {len(en_skills)} skills")
print(f"✅ German: {len(de_skills)} skills")

all_skills = sorted(set(en_skills + de_skills))
print(f"✅ Total combined: {len(all_skills)} skills")

with open('src/ai/skills-esco.json', 'w', encoding='utf-8') as f:
    json.dump(all_skills, f, indent=2, ensure_ascii=False)

print("✅ Saved to src/ai/skills-esco.json")
