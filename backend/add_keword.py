import pandas as pd
import ast

# 1. 기존 키워드 세트
STOCK_SEARCH_KEYWORDS = set([
    "코스피", "코스닥", "환율", "금리인상", "FOMC", "외국인 순매수", "반도체",
    "HBM", "AI반도체", "2차전지", "바이오", "제약", "밸류업", "기업 실적"
])

# 2. 최신 뉴스 데이터 로드 (경로는 각자 맞게 수정)
CSV_PATH = r"P:\stock_crawl\backend\output\merged_no_duplicate.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8")

# 3. 최근 7일 또는 30일 등 원하는 기간만 필터링 (예: 30일)
df['published_at'] = pd.to_datetime(df['published_at'])
date_limit = pd.Timestamp.now() - pd.Timedelta(days=30)
df = df[df['published_at'] >= date_limit]

# 4. analysis_keywords 컬럼에서 전체 키워드 추출 및 집계
all_keywords = []
for x in df['analysis_keywords']:
    # 문자열을 리스트로 변환
    try:
        kws = ast.literal_eval(str(x))
        all_keywords.extend([k for k in kws if isinstance(k, str)])
    except Exception:
        continue

# 5. 집계 (Series.value_counts)
import collections
keyword_counts = collections.Counter(all_keywords)

# 6. 기존 키워드에는 없는 인기 키워드 추출 (최대 20개 예시)
recommended_keywords = [
    (kw, count)
    for kw, count in keyword_counts.most_common(50)
    if kw not in STOCK_SEARCH_KEYWORDS and len(kw) > 1
][:20]

print("💡 추천 검색 키워드 (최근 30일 기준, 기존에 없는 것):")
for kw, count in recommended_keywords:
    print(f"- {kw} ({count}회)")

# 필요시 결과를 txt/csv로 저장해도 됨
