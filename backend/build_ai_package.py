# build_ai_package.py
import pandas as pd, ast, collections, json, os
from datetime import datetime, timedelta

CSV_PATH = r"P:\stock_crawl\backend\output\merged_no_duplicate.csv"
OUT_JSON = r"P:\stock_crawl\backend\output\ai_daily_package.json"

df = pd.read_csv(CSV_PATH, encoding="utf-8")
# --- 여기를 수정: published_at을 date 타입으로 ---
df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce').dt.date

today = datetime.now().date()
recent_limit = today - timedelta(days=7)
prev_limit   = recent_limit - timedelta(days=7)

recent = df[df['published_at'] >= recent_limit]
prev   = df[(df['published_at'] < recent_limit) & (df['published_at'] >= prev_limit)]

# 이하 기존 로직 그대로...
def flat(like_list):
    out=[]
    for x in like_list:
        try: out+=ast.literal_eval(str(x))
        except: pass
    return [i for i in out if isinstance(i,str)]

def get_trending(col_name, recent, prev, topn=10):
    rc = collections.Counter(flat(recent[col_name]))
    pc = collections.Counter(flat(prev[col_name]))
    items=[]
    for k,v in rc.items():
        if v>=3 and v>pc.get(k,0):
            items.append((k,v-pc.get(k,0)))
    return [k for k,_ in sorted(items,key=lambda x:x[1],reverse=True)[:topn]]

trending_kw    = get_trending('analysis_keywords', recent, prev)
trending_stock = get_trending('analysis_orgs', recent, prev)
sent = recent['sentiment_label'].value_counts(normalize=True).round(2).to_dict()
top_articles = recent.sort_values(['sentiment_label','published_at'], ascending=[True,False])\
                      .head(10)[['title','summary_ai','url','sentiment_label']].to_dict('records')
sector = collections.Counter(flat(recent['analysis_keywords'])).most_common(10)
sector_briefs=[{"keyword":k,"mentions":v} for k,v in sector]

package = {
    "date": str(today),
    "trending_keywords": trending_kw,
    "trending_stocks": trending_stock,
    "sentiment_ratio": sent,
    "top_articles": top_articles,
    "sector_briefs": sector_briefs
}

os.makedirs(os.path.dirname(OUT_JSON), exist_ok=True)
with open(OUT_JSON,"w",encoding="utf-8") as f:
    json.dump(package,f,ensure_ascii=False,indent=2)

print(f"✅ AI 패키지 저장 완료 → {OUT_JSON}")
