import re
import pandas as pd
import psycopg2
from openai import OpenAI

# ----------- CONFIG -----------

DB_CONFIG = {
    "host": "law-db.ctuc4yamimfz.ap-southeast-2.rds.amazonaws.com",
    "port": 5432,
    "database": "lawdb",
    "user": "postgres",
    "password": "postgres"
}

openai_api_key = ""  # Replace this with your actual OpenAI API key
client = OpenAI(api_key=openai_api_key)

MAX_ARTICLES = 47  # Only process first 3 for testing

# ----------- AI Functions -----------

def detect_loai_thong_tin(text):
    prompt = f"""Dựa trên nội dung sau, hãy phân loại loại thông tin phù hợp nhất theo 1 trong 4 giá trị sau:
1- Thông tin chung
2- Mô tả hướng dẫn
3- Mô tả phạt
4- Mô tả tham chiếu

Nội dung: \"{text}\"
Chỉ trả về một số (1, 2, 3 hoặc 4)."""
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return int(response.choices[0].message.content.strip())

def detect_doituong(text):
    prompt = f"""Phân tích nội dung sau và xác định đối tượng được nhắc đến (ví dụ: bên mua bảo hiểm, doanh nghiệp bảo hiểm, người được bảo hiểm...):
\"{text}\"
Trả lời bằng cụm từ đối tượng duy nhất. Nếu không rõ, trả về NULL."""
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content.strip()

def detect_thoihan(text):
    prompt = f"""Tìm trong nội dung sau có đề cập thời hạn cụ thể hay không. Nếu có thì trích ra (ví dụ: '01 năm kể từ ngày xảy ra sự kiện bảo hiểm'), nếu không thì trả về NULL:
\"{text}\"
Trả lời chỉ là chuỗi thời hạn hoặc NULL."""
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content.strip()

# ----------- Parse Law.txt -----------

with open("Law.txt", "r", encoding="utf-8") as f:
    lines = f.readlines()

articles, clauses, points = [], [], []
article_id = clause_id = point_id = 0
re_article = re.compile(r"^Điều\s+(\d+)\.\s*(.*)")
re_clause = re.compile(r"^(\d+)\.\s+(.*)")
re_point = re.compile(r"^([a-zA-Zđ])\)\s+(.*)")

current_article = None
current_clause = None
current_point = None

for line in lines:
    line = line.strip()
    if not line:
        continue

    if (m := re_article.match(line)):
        article_id += 1
        current_article = {
            "DieuLuatID": article_id,
            "SoHieuDieu": int(m.group(1)),
            "TenDieuLuat": m.group(2),
            "SoKhoan": 0,
            "NoiDungDieu": "",
        }
        articles.append(current_article)
        current_clause = None
        current_point = None
        continue

    if (m := re_clause.match(line)):
        clause_id += 1
        current_clause = {
            "DieuKhoanID": clause_id,
            "DieuLuatID": article_id,
            "SoHieuKhoan": m.group(1),
            "NoiDungKhoan": m.group(2),
            "SoDiem": 0,
        }
        clauses.append(current_clause)
        articles[-1]["SoKhoan"] += 1
        current_point = None
        continue

    if (m := re_point.match(line)):
        point_id += 1
        current_point = {
            "DiemID": point_id,
            "DieuKhoanID": clause_id,
            "SoHieuDiem": m.group(1),
            "NoiDungDiem": m.group(2)
        }
        points.append(current_point)
        clauses[-1]["SoDiem"] += 1
        continue

    # continuation
    if current_point:
        if re_point.match(line): continue
        current_point = None

    if current_clause:
        current_clause["NoiDungKhoan"] += " " + line

# ----------- Filter for test sample -----------

selected_articles = articles[:MAX_ARTICLES]
selected_article_ids = {a["DieuLuatID"] for a in selected_articles}
selected_clauses = [c for c in clauses if c["DieuLuatID"] in selected_article_ids]
selected_clause_ids = {c["DieuKhoanID"] for c in selected_clauses}
selected_points = [p for p in points if p["DieuKhoanID"] in selected_clause_ids]

# ----------- Insert to DB -----------

def insert_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    for article in selected_articles:
    # Ghép tiêu đề và nội dung điều luật lại để tăng ngữ cảnh cho AI
        combined_text = f"{article['TenDieuLuat'].strip()} {article['NoiDungDieu'].strip()}"

        try:
            loai_id_raw = detect_loai_thong_tin(combined_text)
            loai_id = int(loai_id_raw)
            print(f" [Article {article['SoHieuDieu']}] LoaiThongTinID = {loai_id}")
        except Exception as e:
            print(f"❌ Error detecting LoaiThongTin for Article {article['SoHieuDieu']}: {e}")
            continue  # Bỏ qua article nếu lỗi để không chặn toàn bộ tiến trình
        
        doi_tuong = detect_doituong(combined_text)
        thoi_han = detect_thoihan(combined_text)

        cur.execute("""
            INSERT INTO DieuLuat (DieuLuatID, LoaiThongTinID, TenDieuLuat, SoHieuDieu, SoKhoan, NoiDungDieu, DoiTuong, ThoiHan)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            article["DieuLuatID"], loai_id, article["TenDieuLuat"],
            article["SoHieuDieu"], article["SoKhoan"], article["NoiDungDieu"],
            "dân sự", None
        ))
        print(f" [Article {article['SoHieuDieu']}]/61")


    for clause in selected_clauses:
        try:
            loai_id_raw = detect_loai_thong_tin(clause["NoiDungKhoan"])
            loai_id = int(loai_id_raw)
            print(f"   [Clause {clause['SoHieuKhoan']}] LoaiThongTinID = {loai_id}")
        except Exception as e:
            print(f"  ❌ Error detecting LoaiThongTin for Clause {clause['SoHieuKhoan']}: {e}")
            continue
        doi_tuong = detect_doituong(clause["NoiDungKhoan"])
        thoi_han = detect_thoihan(clause["NoiDungKhoan"])

        cur.execute("""
            INSERT INTO DieuKhoan (DieuKhoanID, DieuLuatID, LoaiThongTinID, SoHieuKhoan, SoDiem, NoiDungKhoan, DoiTuong, ThoiHan)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            clause["DieuKhoanID"], clause["DieuLuatID"], loai_id,
            clause["SoHieuKhoan"], clause["SoDiem"], clause["NoiDungKhoan"],
            doi_tuong if doi_tuong != "NULL" else None,
            thoi_han if thoi_han != "NULL" else None
        ))
        print(f" [Article {article['SoHieuDieu']} - {clause['SoHieuKhoan']}]")


    for point in selected_points:
        try:
            loai_id_raw = detect_loai_thong_tin(point["NoiDungDiem"])
            loai_id = int(loai_id_raw)
            print(f"     [Point {point['SoHieuDiem']}] LoaiThongTinID = {loai_id}")
        except Exception as e:
            print(f"    ❌ Error detecting LoaiThongTin for Point {point['SoHieuDiem']}: {e}")
            continue

        doi_tuong = detect_doituong(point["NoiDungDiem"])
        thoi_han = detect_thoihan(point["NoiDungDiem"])

        cur.execute("""
            INSERT INTO Diem (DiemID, DieuKhoanID, LoaiThongTinID, SoHieuDiem, NoiDungDiem, DoiTuong, ThoiHan)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            point["DiemID"], point["DieuKhoanID"], loai_id,
            point["SoHieuDiem"], point["NoiDungDiem"],
            doi_tuong if doi_tuong != "NULL" else None,
            thoi_han if thoi_han != "NULL" else None
        ))
        print(f"[Article {article['SoHieuDieu']} - {clause['SoHieuKhoan']}- {point['SoHieuDiem']}]")


    conn.commit()
    cur.close()
    conn.close()
    print("\n✅ Done: Data inserted into RDS")

insert_data()
