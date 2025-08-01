import pandas as pd
import glob
import os

# === 1. 파일 경로 지정 ===
FOLDER = r'P:\stock_crawl\backend\output'  # 파일들이 모여있는 폴더 경로
file_list = glob.glob(os.path.join(FOLDER, "*.csv"))

# === 2. 취합할 컬럼명(순서 고정) ===
keep_columns = ["url", "title", "published_at", "analysis_keywords", "analysis_orgs", "summary_ai", "sentiment_label"]

# === 3. 파일별로 읽어서 필요한 컬럼만 추출 ===
df_list = []
for file in file_list:
    try:
        df = pd.read_csv(file, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(file, encoding="utf-8-sig")
    # 필요한 컬럼만 선택(없는 컬럼은 NaN으로 채워짐)
    sub = pd.DataFrame()
    for col in keep_columns:
        if col in df.columns:
            sub[col] = df[col]
        else:
            sub[col] = None
    df_list.append(sub)

# === 4. 데이터 합치기 + url 기준 중복제거 ===
merged = pd.concat(df_list, ignore_index=True)
merged = merged.drop_duplicates(subset="url")

# === 5. 저장 (컬럼 순서 유지) ===
output_file = os.path.join(FOLDER, "merged_no_duplicate.csv")
merged.to_csv(output_file, index=False, encoding="utf-8-sig", columns=keep_columns)

print(f"완료! 총 {len(merged)}건의 데이터가 중복 없이 합쳐졌습니다.\n→ 저장 위치: {output_file}")
