# 교체할 코드: 파일 상단 import 영역
import os
import sys
import json
import time
from datetime import datetime, timedelta
import re
import warnings
import urllib3
import csv
import ast
import collections

# --- 필수 라이브러리 임포트 ---
import requests
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv  # <-- 추가
from tqdm import tqdm          # <-- 추가
import google.generativeai as genai
import google.api_core.exceptions

# --- .env 파일에서 환경 변수 로드 ---
load_dotenv() # <-- 추가

# --- SSL 경고 비활성화 ---
warnings.filterwarnings("ignore")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# ==============================================================================
# 🚀 설정 영역
# ==============================================================================
# 교체할 코드: 🚀 설정 영역
# ==============================================================================
# 🚀 설정 영역
# ==============================================================================
# 교체할 코드: 🚀 설정 영역
# ==============================================================================
# 🚀 설정 영역
# ==============================================================================

# --- API 키 및 ID (.env 파일을 통해 환경 변수로 전달받음) ---
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

# --- 데이터 수집 기간 및 개수 설정 ---
DATA_COLLECTION_DAYS = 1      # 수집할 기간 (일)
ARTICLES_PER_DAY_LIMIT = 100    # 키워드당 하루에 수집할 최대 기사 수

# --- 검색 키워드 목록 ---
STOCK_SEARCH_KEYWORDS = [
    "코스피", "코스닥", "환율", "금리인상", "FOMC", "외국인 순매수", "반도체",
    "HBM", "AI반도체", "2차전지", "바이오", "제약", "밸류업", "기업 실적"
]
# --- 기타 설정 ---
RATE_LIMIT_DELAY = 1
BATCH_SIZE = 7
ARTICLE_END_MARKERS = [
    "무단전재", "무단 전재", "재배포 금지", "저작권자", "광고문의", 
    "광고 문의", "AD링크", "타불라", "관련기사", "기자소개", "기자 소개",
    "기자의 다른기사", "편집패널", "본문하단", "nBYLINE", "좋아요 버튼",
    "속보는", "t.me/", "텔레그램", "영상취재", "기사제보", "보도자료",
    "팟캐스트", "많이 본 기사", "공유하기", "공유버튼", "nCopyright",
    "기사 전체보기", "입력 :", "지면 :", "AI학습 이용 금지", "기사 공유",
    "댓글", "좋아요", "광고", "관련 뉴스", "추천 뉴스", "영상편집",
    "뉴스제공", "기사제공", "기사 하단 광고", "기사 영역 하단 광고",
    "기자 정보", "전체기사 보기", "장기영 기자", "공감언론",
    "기자 (", "기자 =", "기자]", "[사진=", "자료=", "(서울=연합뉴스)",
    "[파이낸셜뉴스]", "페이스북", "트위터", "카카오톡", "제보하기",
    "독자 여러분의 소중한 제보를 기다립니다", "▶", "※", "☞", "[ⓒ", "◎"
]

# ==============================================================================
# 🤖 AI 및 프롬프트 함수
# ==============================================================================
gemini_model = None

def initialize_gemini_model():
    """Gemini 모델을 초기화합니다."""
    global gemini_model
    if not GOOGLE_API_KEY:
        raise ValueError("Google API 키가 설정되지 않았습니다.")
    try:
        genai.configure(api_key=GOOGLE_API_KEY)
        gemini_model = genai.GenerativeModel("models/gemini-1.5-flash")
        print("✅ Gemini 모델 초기화 성공")
    except Exception as e:
        print(f"🚨 Gemini 모델 초기화 실패: {e}")
        raise

def get_stock_analysis_prompt(content):
    """주식/경제 뉴스 분석을 위한 프롬프트를 생성합니다."""
    return f"""
당신은 최고의 금융 뉴스 분석가입니다. 아래에 제공되는 여러 개의 뉴스 기사들을 분석하여, 각 기사별로 지정된 JSON 형식에 맞춰 주요 정보를 추출해주세요.

[분석 규칙]
- 각 기사는 `<article>` 태그로 구분되며, 각 기사의 `<id>`를 JSON 결과의 "id" 필드에 반드시 포함시켜야 합니다.
- `analysis_keywords`: 기사의 핵심 주제를 나타내는 키워드를 5개 내외로 추출합니다.
- `analysis_orgs`: 기사에 언급된 주요 '기관/기업(ORG)'을 정확히 추출합니다.
- `summary_ai`: 기사의 핵심 내용을 2~3문장으로 요약합니다.
- `sentiment_label`: 기사의 전반적인 톤이 긍정적인지(Positive), 부정적인지(Negative), 중립적인지(Neutral) 평가합니다.
- 결과는 반드시 전체를 감싸는 단일 JSON 리스트(배열) 형식이어야 하며, 다른 설명 없이 JSON 코드만 출력해야 합니다.

[분석할 기사 목록]
{content}

[출력 JSON 형식]
```json
[
  {{
    "id": "<article>의 id 값>",
    "analysis_keywords": ["키워드1", ...],
    "analysis_orgs": ["기관1", ...],
    "summary_ai": "기사 요약",
    "sentiment_label": "Positive, Negative, 또는 Neutral"
  }},
  ...
]
"""
# 교체할 함수: crawl_naver_news (기존 함수를 통째로 교체)
def crawl_naver_news(keywords, existing_urls):
    """
    지정된 키워드 목록으로 네이버 뉴스를 수집합니다.
    페이지네이션을 통해 날짜별로 지정된 개수만큼 수집하고, 전체 URL 중복을 제거합니다.
    """
    api_url = "https://openapi.naver.com/v1/search/news.json"
    headers = {"X-Naver-Client-Id": NAVER_CLIENT_ID, "X-Naver-Client-Secret": NAVER_CLIENT_SECRET}
    all_new_articles = []
    today = datetime.now()
    
    # 1. 수집 대상 날짜 목록 생성
    target_dates = [(today - timedelta(days=i)).date() for i in range(DATA_COLLECTION_DAYS)]
    target_dates_str = {d.strftime('%Y-%m-%d') for d in target_dates} # 빠른 조회를 위해 set 사용
    
    print(f"\n--- 1단계: 네이버 뉴스 수집 시작 (대상 기간: 최근 {DATA_COLLECTION_DAYS}일, 일별 최대 {ARTICLES_PER_DAY_LIMIT}개) ---")

    # 2. 각 키워드에 대해 수집 시작
    for keyword in keywords:
        print(f"\n 🔎 키워드 '{keyword}' 수집 중...")
        
        # 키워드별로 날짜당 몇 개를 수집했는지 카운트
        daily_counts = {date_str: 0 for date_str in target_dates_str}
        
        start_index = 1
        keep_searching_for_keyword = True

        # 3. 페이지네이션 루프 (API의 start 값을 1, 101, 201... 순으로 증가)
        while keep_searching_for_keyword and start_index <= 1000: # 네이버 API는 최대 1000개까지 조회 가능
            params = {"query": keyword, "display": 100, "start": start_index, "sort": "date"}
            
            try:
                response = requests.get(api_url, headers=headers, params=params, verify=False, timeout=10)
                response.raise_for_status()
                data = response.json()
                items = data.get('items', [])

                if not items:
                    print(f"   - '{keyword}' 키워드에 대한 결과가 더 이상 없습니다.")
                    break # 더 이상 결과가 없으면 이 키워드에 대한 검색 중단

                found_new_in_batch = False
                for item in items:
                    try:
                        pub_date = datetime.strptime(item['pubDate'], '%a, %d %b %Y %H:%M:%S %z').date()
                        pub_date_str = pub_date.strftime('%Y-%m-%d')
                    except (ValueError, TypeError):
                        continue
                        
                    # 수집 대상 날짜가 아니면 건너뛰기
                    if pub_date_str not in target_dates_str:
                        continue
                    
                    # 해당 날짜의 수집 한도를 초과했으면 건너뛰기
                    if daily_counts[pub_date_str] >= ARTICLES_PER_DAY_LIMIT:
                        continue
                        
                    # URL 중복 체크
                    url = item.get('originallink') or item.get('link')
                    if not url or url in existing_urls:
                        continue
                    
                    # 모든 조건을 통과하면 기사 추가
                    title = re.sub('<[^<]+?>', '', item.get('title', ''))
                    summary = re.sub('<[^<]+?>', '', item.get('description', ''))
                    
                    all_new_articles.append({
                        "search_keyword": keyword, "url": url, "title": title, "summary": summary,
                        "crawled_at": datetime.now().isoformat(), "published_at": pub_date_str
                    })
                    
                    existing_urls.add(url) # 전역 중복 방지를 위해 URL 추가
                    daily_counts[pub_date_str] += 1 # 일별 카운트 증가
                    found_new_in_batch = True

                # 다음 페이지로 이동
                start_index += 100
                
                # 모든 날짜에 대해 수집 목표를 달성했는지 체크
                if all(count >= ARTICLES_PER_DAY_LIMIT for count in daily_counts.values()):
                    print(f"   - '{keyword}' 키워드의 모든 날짜별 수집 목표를 달성했습니다.")
                    keep_searching_for_keyword = False

                time.sleep(RATE_LIMIT_DELAY)

            except Exception as e:
                print(f" ❌ '{keyword}' 수집 중 오류: {e}")
                break # 오류 발생 시 해당 키워드 검색 중단

    print(f"\n--- ✅ 전체 뉴스 수집 완료. 총 {len(all_new_articles)}개의 새 기사 발견 ---")
    return all_new_articles

def extract_article_content(url):
    """주어진 URL에서 기사 본문을 추출합니다."""
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "ko-KR,ko;q=0.9"}
    try:
        response = requests.get(url, headers=headers, timeout=15, verify=False)
        response.raise_for_status()
        if response.encoding.lower() in ['iso-8859-1', 'euc-kr']:
            response.encoding = response.apparent_encoding
        soup = BeautifulSoup(response.text, "lxml")
        for tag in soup(['script', 'style', 'header', 'footer', 'nav', 'aside', 'iframe', 'figure']):
            tag.decompose()
        selectors = [
            "#article-view-content-div", "#CmAdContent", "#articleBody", "#article-body", "#view_content_wrap",
            "#article-content-body", "#news_body_area", "#article_content", "#news-contents", "#articleText",
            "article", ".article_body", ".news_end"
        ]
        content_area = next((soup.select_one(s) for s in selectors if soup.select_one(s)), None)
        if content_area:
            text = content_area.get_text(separator='\n', strip=True)
            if len(text) > 100:
                lines = text.split('\n')
                cleaned_lines = []
                for line in lines:
                    if any(marker in line for marker in ARTICLE_END_MARKERS):
                        break
                    cleaned_lines.append(line)
                return '\n'.join(cleaned_lines).strip()
        return "[실패] 본문 영역 추출 실패"
    except Exception as e:
        return f"[오류] {str(e)}"
# 추가할 함수 1: 중간 데이터 저장
def save_intermediate_data(articles, path="output/intermediate/crawled_data.csv"):
    """본문 추출까지 완료된 데이터를 CSV 파일로 저장합니다."""
    if not articles:
        print("저장할 데이터가 없습니다.")
        return
    
    print(f"\n--- 💾 중간 저장 단계 ---")
    output_dir = os.path.dirname(path)
    os.makedirs(output_dir, exist_ok=True)
    
    # 데이터프레임으로 변환하여 저장
    df = pd.DataFrame(articles)
    df.to_csv(path, index=False, encoding='utf-8-sig')
    print(f"✅ 본문 추출 완료된 기사 {len(df)}개를 다음 경로에 저장했습니다: {path}")
    return path

# 추가할 함수 2: 중간 데이터 불러오기
def load_intermediate_data(path="output/intermediate/crawled_data.csv"):
    """파일로 저장된 중간 데이터를 불러옵니다."""
    if not os.path.exists(path):
        return None
        
    print(f"\n--- 💾 중간 데이터 로딩 ---")
    print(f"✅ 저장된 중간 데이터 파일을 발견했습니다: {path}")
    print("수집 및 본문 추출 단계를 건너뛰고 이 파일에서 분석을 시작합니다.")
    df = pd.read_csv(path)
    # CSV로 저장되면서 문자열이 된 컬럼들을 다시 원래 타입으로 변환
    # 이 예제에서는 모든 컬럼이 문자열이므로, DataFrame을 바로 리스트(dict)로 변환
    articles = df.to_dict('records')
    return articles

def analyze_articles_with_ai(articles):
    """기사 목록을 AI를 통해 분석하고 구조화된 데이터를 추가합니다."""
    print("\n--- 3단계: AI 구조화 분석 시작 ---")
    articles_to_process, article_map = [], {}
    for i, article in enumerate(articles):
        article['unique_id'] = f"art_{i}"
        article_map[article['unique_id']] = article
        content_to_analyze = article.get("content", "")
        if not content_to_analyze or content_to_analyze.startswith(("[실패]", "[오류]")):
            content_to_analyze = article.get("summary", "")
        if content_to_analyze:
            article['content_to_analyze'] = content_to_analyze
            articles_to_process.append(article)
    print(f"  - AI 분석 대상: {len(articles_to_process)}개 / 총 {len(articles)}개")
    if not articles_to_process:
        return articles

    for i in range(0, len(articles_to_process), BATCH_SIZE):
        batch = articles_to_process[i:i + BATCH_SIZE]
        print(f"  - 배치 {i//BATCH_SIZE + 1} 처리 중... ({len(batch)}개)")
        batch_content = "\n\n".join([
            f"<article>\n<id>{art['unique_id']}</id>\n<content>\n{art['content_to_analyze']}\n</content>\n</article>"
            for art in batch
        ])
        final_prompt = get_stock_analysis_prompt(batch_content)

        try:
            response = gemini_model.generate_content(final_prompt)
            cleaned_response = response.text.strip().lstrip("```json").lstrip("```").rstrip("```")
            analysis_results_list = json.loads(cleaned_response)
            if isinstance(analysis_results_list, list):
                for result in analysis_results_list:
                    if result.get('id') in article_map:
                        article_map[result['id']].update(result)
            else:
                print(f"    ⚠️ 배치 {i//BATCH_SIZE + 1} 분석 결과가 리스트가 아님.")
        except Exception as e:
            print(f"    - 배치 분석 중 오류: {e}")

    final_list = list(article_map.values())
    for art in final_list:
        art.pop('unique_id', None)
        art.pop('id', None)
        art.pop('content_to_analyze', None)
    print("--- ✅ AI 분석 완료 ---")
    return final_list

# ==============================================================================
# 💾 4단계: 데이터 취합 및 CSV 저장 함수 (JSONBin 대신 파일로 저장)
# ==============================================================================
def aggregate_and_save_to_csv(new_articles, output_dir):
    """새로운 기사를 로컬 CSV 파일에 누적하여 저장합니다."""
    print("\n--- 4단계: 데이터 병합 및 CSV 저장 시작 ---")
    if not new_articles:
        print("  - 취합할 새 데이터가 없습니다.")
        return

    # 이 스크립트는 항상 최신 3일치 데이터를 가져오므로, 
    # 기존 데이터를 읽어와 병합하는 대신 매번 새로 만드는 것이 더 간단하고 안정적입니다.
    # (어차피 대시보드는 최신 데이터만 보여줄 것이므로)
    
    # 1. DataFrame으로 변환
    df = pd.DataFrame(new_articles)
    
    # 2. 오래된 데이터 제거 (예: 최근 30일치 데이터만 유지)
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce').dt.strftime('%Y-%m-%d')
    final_df = df[df['published_at'] >= thirty_days_ago].copy()
    
    # 3. 리스트 형태의 컬럼을 CSV에 저장하기 좋게 문자열로 변환
    for col in ['analysis_orgs', 'analysis_keywords']:
        if col in final_df.columns:
            final_df[col] = final_df[col].apply(lambda x: str(x) if isinstance(x, list) else str(x))

    # 4. CSV 파일로 저장
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, "aggregated_stock_data.csv")
    final_df.to_csv(csv_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
    print(f"--- ✅ CSV 저장 완료. 총 {len(final_df)}개 기사 저장 ---")
    print(f"   - 저장 경로: {csv_path}")

# 교체할 함수: main (기존 함수를 통째로 교체)
def main():
    """
    전체 파이프라인을 순서대로 실행하는 메인 함수입니다.
    스크립트 위치를 기준으로 파일 경로를 지정하여 안정성을 높였습니다.
    """
    print("="*60)
    print(" K-Stock News Analysis Pipeline (Local, Resumable) - START")
    print("="*60)

    # --- [핵심 수정] 스크립트 파일의 실제 위치를 기준으로 경로 설정 ---
    # 1. 스크립트 파일이 있는 디렉토리의 절대 경로를 가져옵니다.
    #    (예: P:\stock_crawl\backend)
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

    # 2. 이 디렉토리를 기준으로 중간 및 최종 저장 경로를 생성합니다.
    #    (예: P:\stock_crawl\backend\output\intermediate\crawled_data.csv)
    intermediate_file_path = os.path.join(SCRIPT_DIR, "output", "intermediate", "crawled_data.csv")
    final_output_dir = os.path.join(SCRIPT_DIR, "output", "aggregated")
    # -----------------------------------------------------------------

    # 중간 데이터 파일이 있는지 확인
    analyzed_articles = None
    articles_to_process = load_intermediate_data(intermediate_file_path)

    # 중간 파일이 없으면, 수집부터 시작
    if articles_to_process is None:
        print("\n중간 데이터 파일이 없습니다. 뉴스 수집부터 새로 시작합니다.")
        try:
            initialize_gemini_model()
        except Exception as e:
            print(f"❌ 초기화 중 치명적 오류 발생: {e}")
            return

        temp_existing_urls = set()
        new_articles = crawl_naver_news(STOCK_SEARCH_KEYWORDS, temp_existing_urls)
        
        if not new_articles:
            print("\n✅ 수집된 새로운 뉴스가 없습니다. 파이프라인을 종료합니다.")
            return
            
        print("\n--- 2단계: 기사 본문 추출 시작 ---")
        for article in tqdm(new_articles, desc="  - 본문 추출 중"):
            if not article.get('content'):
                article['content'] = extract_article_content(article.get('url', ''))
                time.sleep(0.1)
        print("--- ✅ 본문 추출 완료 ---")

        # 본문 추출 후, AI 분석 전에 중간 파일로 저장
        save_intermediate_data(new_articles, intermediate_file_path)
        articles_to_process = new_articles
    
    # AI 분석 실행 (새로 수집했거나, 파일에서 불러왔거나)
    if articles_to_process:
        try:
            if 'gemini_model' not in globals() or gemini_model is None:
                initialize_gemini_model()
            
            analyzed_articles = analyze_articles_with_ai(articles_to_process)
            
            aggregate_and_save_to_csv(analyzed_articles, final_output_dir)

            if os.path.exists(intermediate_file_path):
                os.remove(intermediate_file_path)
                print(f"\n✅ 최종 분석 완료. 중간 파일({intermediate_file_path})을 삭제했습니다.")

        except Exception as e:
            print("\n" + "="*60)
            print(f"🚨 AI 분석 또는 최종 저장 단계에서 오류가 발생했습니다: {e}")
            print(f"👍 하지만 걱정마세요! 수집된 데이터는 '{intermediate_file_path}'에 안전하게 저장되어 있습니다.")
            print("   스크립트를 다시 실행하면 저장된 데이터로 AI 분석을 재시도합니다.")
            print("="*60)
            return

    print("\n" + "="*60)
    print(" K-Stock News Analysis Pipeline - COMPLETE")
    print("="*60)
# 교체할 코드: if __name__ == "__main__": 블록 전체
if __name__ == "__main__":
    main()

    # (추가) 키워드 집계 및 추천
    print("\n[추천 키워드 분석]")
    try:
        latest_path = os.path.join("output", "aggregated", "aggregated_stock_data.csv")
        if not os.path.exists(latest_path):
             print("분석할 CSV 파일이 없습니다.")
        else:
            df = pd.read_csv(latest_path, encoding='utf-8')
            df.dropna(subset=['analysis_keywords'], inplace=True) # 키워드가 없는 행 제거

            all_keywords = []
            # [수정] ast.literal_eval 오류 발생 시 건너뛰도록 예외 처리 강화
            for x in df['analysis_keywords']:
                try:
                    kws = ast.literal_eval(str(x))
                    if isinstance(kws, list):
                        all_keywords.extend([k for k in kws if isinstance(k, str)])
                except (ValueError, SyntaxError):
                    continue

            keyword_counts = collections.Counter(all_keywords)
            recommended_keywords = [
                (kw, count)
                for kw, count in keyword_counts.most_common(50)
                if kw not in STOCK_SEARCH_KEYWORDS and len(kw) > 1
            ][:20]

            # [수정] DATA_COLLECTION_DAYS 변수를 사용하여 메시지 출력
            print(f"💡 최근 {DATA_COLLECTION_DAYS}일간 추천 검색 키워드:")
            for kw, count in recommended_keywords:
                print(f"- {kw} ({count}회)")

    except Exception as e:
        print(f"[키워드 추천 분석 오류] {e}")