import pandas as pd
import ast
import collections

# 기존 키워드 리스트
STOCK_SEARCH_KEYWORDS = ["코스피", "연준", "삼성전자", "2분기 실적", 
"주가 상승", "파월 의장", "반도체", "금리 동결", "한미 관세 협상", "코스닥"]

# 기사 데이터 로드
CSV_PATH = r"P:\stock_crawl\backend\output\merged_no_duplicate.csv"
df = pd.read_csv(CSV_PATH, encoding="utf-8")
df['published_at'] = pd.to_datetime(df['published_at'])
date_limit = pd.Timestamp.now() - pd.Timedelta(days=30)
df = df[df['published_at'] >= date_limit]

# 키워드 전체 집계
all_keywords = []
for x in df['analysis_keywords']:
    try:
        kws = ast.literal_eval(str(x))
        all_keywords.extend([k for k in kws if isinstance(k, str)])
    except Exception:
        continue
keyword_counts = collections.Counter(all_keywords)

# 기존 키워드별 등장 빈도
print("\n[기존 키워드별 최근 30일 기사 등장수]")
for kw in STOCK_SEARCH_KEYWORDS:
    print(f"{kw}: {keyword_counts[kw]}회")

# “중요도 낮은 키워드” 자동 정리(3회 이하 등 임계값 적용)
LOW_IMPORTANCE = [kw for kw in STOCK_SEARCH_KEYWORDS if keyword_counts[kw] <= 3]
print("\n💡 최근 30일간 거의 등장하지 않은 '정리 추천' 키워드:")
for kw in LOW_IMPORTANCE:
    print(f"- {kw}")

# "상위 N개만 남기고 나머지 자동 제거" 예시 (N=10)
N = 10
top_keywords = [kw for kw, _ in keyword_counts.most_common(N)]
print(f"\n💡 추천 상위 {N}개 키워드만 남기기:", top_keywords)
