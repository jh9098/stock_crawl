# backend/run_pipeline.py
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
from datetime import datetime, timedelta
import re
import warnings
import urllib3

# --- 필수 라이브러리 임포트 ---
import requests
import pandas as pd
from bs4 import BeautifulSoup
import google.generativeai as genai
import google.api_core.exceptions

# --- SSL 경고 비활성화 ---
warnings.filterwarnings("ignore")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==============================================================================
# 🚀 설정 영역
# ==============================================================================

# --- API 키 및 ID (GitHub Secrets를 통해 환경 변수로 전달받음) ---
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")
JSONBIN_API_KEY = os.getenv("JSONBIN_API_KEY")
JSONBIN_BIN_ID = os.getenv("JSONBIN_BIN_ID")

# --- 검색 키워드 목록 ---
STOCK_SEARCH_KEYWORDS = [
    "코스피", "코스닥", "환율", "금리인상", "FOMC", "외국인 순매수", "반도체",
    "HBM", "AI반도체", "2차전지", "바이오", "제약", "밸류업", "기업 실적"
]

# --- 기타 설정 ---
RATE_LIMIT_DELAY = 1
BATCH_SIZE = 5
ARTICLE_END_MARKERS = [
    "무단전재", "무단 전재", "재배포 금지", "저작권자", "광고문의", "기자=", "기자 (",
    "기자]", "[사진=", "(서울=연합뉴스)", "[ⓒ", "AI학습 이용 금지", "Copyright"
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
def crawl_naver_news(keywords, existing_urls):
    """지정된 키워드 목록으로 네이버 뉴스를 수집합니다."""
    api_url = "https://openapi.naver.com/v1/search/news.json"
    headers = {"X-Naver-Client-Id": NAVER_CLIENT_ID, "X-Naver-Client-Secret": NAVER_CLIENT_SECRET}
    all_new_articles = []
    today = datetime.now()
    # [수정] 최근 3일치 데이터를 가져오도록 범위 확장
    target_dates = [(today - timedelta(days=i)).date() for i in range(3)]
    date_str = ", ".join([d.strftime('%Y-%m-%d') for d in target_dates])
    print(f"\n--- 1단계: 네이버 뉴스 수집 시작 (대상 날짜: {date_str}) ---")
    for keyword in keywords:
        print(f" 🔎 키워드 '{keyword}' 수집 중...")
        params = {"query": keyword, "display": 100, "start": 1, "sort": "date"}
        try:
            response = requests.get(api_url, headers=headers, params=params, verify=False, timeout=10)
            response.raise_for_status()
            data = response.json()
            items = data.get('items', [])
            if not items:
                continue
            for item in items:
                try:
                    pub_date = datetime.strptime(item['pubDate'], '%a, %d %b %Y %H:%M:%S %z').date()
                except (ValueError, TypeError):
                    continue
                if pub_date not in target_dates:
                    continue
                url = item.get('originallink') or item.get('link')
                if not url or url in existing_urls:
                    continue
                title = re.sub('<[^<]+?>', '', item.get('title', ''))
                summary = re.sub('<[^<]+?>', '', item.get('description', ''))
                all_new_articles.append({
                    "search_keyword": keyword, "url": url, "title": title, "summary": summary,
                    "crawled_at": datetime.now().isoformat(), "published_at": pub_date.strftime('%Y-%m-%d')
                })
                existing_urls.add(url)
            time.sleep(RATE_LIMIT_DELAY)
        except Exception as e:
            print(f" ❌ '{keyword}' 수집 중 오류: {e}")
    print(f"--- ✅ 뉴스 수집 완료. 총 {len(all_new_articles)}개의 새 기사 발견 ---")
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

def get_existing_data_from_bin():
    """JSONBin에서 기존 데이터를 가져옵니다. 항상 리스트를 반환하도록 보장합니다."""
    print("  - 원격 저장소(JSONBin.io)에서 기존 데이터 다운로드 중...")
    headers = {'X-Master-Key': JSONBIN_API_KEY, 'X-Bin-Versioning': 'false'}
    url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}/latest"
    try:
        req = requests.get(url, headers=headers, timeout=15)
        req.raise_for_status()
        response_data = req.json()

        # [핵심 수정] JSONBin.io 응답 구조에 맞춰 안정적으로 데이터 추출
        # 최상위가 딕셔너리이고 'record' 키가 있으면 그 안의 리스트를 사용
        if isinstance(response_data, dict) and 'record' in response_data:
            result = response_data['record']
        # 최상위가 리스트이면 그대로 사용
        elif isinstance(response_data, list):
            result = response_data
        # 그 외의 경우 비정상으로 간주
        else:
            print("  - ⚠️ 원격 데이터가 예상치 못한 형식입니다. 빈 리스트로 처리합니다.")
            result = []

        # 최종 결과가 리스트인지 한번 더 확인
        if not isinstance(result, list):
             print(f"  - ⚠️ 최종 추출 결과가 리스트가 아닙니다 (타입: {type(result)}). 빈 리스트로 처리합니다.")
             return []

        print(f"  - 기존 데이터 다운로드 성공. ({len(result)}개)")
        return result
        
    except json.JSONDecodeError:
        print("  - ⚠️ 원격 데이터가 비어 있거나 JSON 형식이 아닙니다. 빈 리스트로 처리합니다.")
        return []
    except Exception as e:
        print(f"  - ⚠️ 기존 데이터 다운로드 실패 (아마도 첫 실행): {e}")
        return []
def upload_data_to_bin(data):
    """JSONBin에 최종 데이터를 업로드(덮어쓰기)합니다."""
    print(" - 원격 저장소(JSONBin.io)에 최종 데이터 업로드 중...")
    headers = {'Content-Type': 'application/json', 'X-Master-Key': JSONBIN_API_KEY}
    url = f"https://api.jsonbin.io/v3/b/{JSONBIN_BIN_ID}"
    try:
        req = requests.put(url, json=data, headers=headers, timeout=15)
        req.raise_for_status()
        print(f" - ✅ 최종 데이터 업로드 성공! (총 {len(data)}개)")
    except Exception as e:
        print(f" - 🚨 최종 데이터 업로드 실패: {e}")

def aggregate_data(new_articles):
    """새로운 기사와 기존 기사를 병합하고 중복을 제거합니다."""
    print("\n--- 4단계: 데이터 병합 및 업로드 시작 ---")
    if not new_articles:
        print(" - 취합할 새 데이터가 없습니다.")
        return
    # 1. 원격 저장소에서 기존 데이터 가져오기
    existing_articles = get_existing_data_from_bin()

    # 2. DataFrame으로 변환하여 병합 및 중복 제거
    df_new = pd.DataFrame(new_articles)
    df_existing = pd.DataFrame(existing_articles)

    combined_df = pd.concat([df_existing, df_new], ignore_index=True)
    combined_df.drop_duplicates(subset=['url'], keep='last', inplace=True)

    # 3. 오래된 데이터 제거 (예: 최근 30일치 데이터만 유지)
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    combined_df['published_at'] = pd.to_datetime(combined_df['published_at'], errors='coerce').dt.strftime('%Y-%m-%d')
    final_df = combined_df[combined_df['published_at'] >= thirty_days_ago]

    print(f"  - 데이터 병합 완료. (기존: {len(df_existing)}, 신규: {len(df_new)}, 중복제거 후: {len(combined_df)}, 최종: {len(final_df)})")

    # 4. 다시 JSON 리스트 형태로 변환하여 업로드
    final_data = final_df.to_dict(orient='records')
    upload_data_to_bin(final_data)

def main():
    """전체 파이프라인을 순서대로 실행하는 메인 함수입니다."""
    print("="*50)
    print(" K-Stock News Analysis Pipeline (GitHub Actions) - START")
    print("="*50)
    try:
        initialize_gemini_model()
    except Exception as e:
        print(f"❌ 초기화 중 오류 발생: {e}")
        sys.exit(1) # 초기화 실패 시 실행 중단

    # 기존 데이터(URL)는 JSONBin에서 직접 가져와 중복을 체크
    existing_articles = get_existing_data_from_bin()
    existing_urls = {article.get('url') for article in existing_articles}

    new_articles = crawl_naver_news(STOCK_SEARCH_KEYWORDS, existing_urls)

    if not new_articles:
        print("\n✅ 수집된 새로운 뉴스가 없습니다. 파이프라인을 종료합니다.")
        return

    print("\n--- 2단계: 기사 본문 추출 시작 ---")
    for i, article in enumerate(new_articles):
        if not article.get('content'):
            print(f"  - ({i+1}/{len(new_articles)}) 본문 추출 중: {article.get('url', '')[:70]}...")
            article['content'] = extract_article_content(article.get('url', ''))
            time.sleep(0.1)
    print("--- ✅ 본문 추출 완료 ---")

    analyzed_articles = analyze_articles_with_ai(new_articles)

    aggregate_data(analyzed_articles)

    print("\n" + "="*50)
    print(" K-Stock News Analysis Pipeline - COMPLETE")
    print("="*50)

if __name__ == "__main__":
    main()