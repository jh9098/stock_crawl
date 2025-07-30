# backend/run_pipeline.py
# -*- coding: utf-8 -*-
import csv
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
    "HBM", "AI반도체", "2차전지", "바이오", "제약", "밸류업", "기업 실적", "어닝 서프라이즈", "전쟁"
]

# --- 기타 설정 ---
RATE_LIMIT_DELAY = 1
BATCH_SIZE = 10
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
        params = {"query": keyword, "display": 30, "start": 1, "sort": "date"}
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

# ==============================================================================
# 📰 2단계: 기사 본문 추출 함수 (최적화 버전 적용)
# ==============================================================================

def clean_bottom_boilerplate(text):
    """기사 하단의 반복적인 상용구(기자 정보, 저작권 등)를 정리합니다."""
    if not isinstance(text, str) or not text: return text
    # 이메일 및 URL 제거
    text = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '', text)
    text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)
    lines = text.split('\n')
    cleaned_lines = []
    for line in lines:
        # END_MARKERS 중 하나가 줄 시작 30자 이내에 나타나면 중단
        found_marker = any(marker in line.strip()[:30] for marker in ARTICLE_END_MARKERS)
        if found_marker: break
        cleaned_lines.append(line)
    return '\n'.join(cleaned_lines).strip()

def extract_article_content(url):
    """
    [최신화 버전] API 처리와 범용 파서를 결합한 통합 본문 추출 함수
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"
    }
    
    # --- 전략 1: 사이트별 API 특별 처리 (Whitelist 방식) ---
    
    # 1. SBS Biz (JSON API)
    if 'biz.sbs.co.kr/article' in url:
        try:
            article_id_match = re.search(r'/(\d{11})', url)
            if not article_id_match: return "[실패] SBS Biz: 기사 ID 없음"
            article_id = article_id_match.group(1)
            api_url = f"https://apis.sbs.co.kr/play-api/1.0/sbs_newsmedia/{article_id}"
            api_response = requests.get(api_url, headers=headers, timeout=10, verify=False)
            api_response.raise_for_status()
            data = api_response.json()
            content_html = data.get('clip', {}).get('info', {}).get('contentdata', '') or \
                           data.get('clip', {}).get('info', {}).get('synopsis', '')
            if content_html:
                soup = BeautifulSoup(content_html, "lxml")
                return clean_bottom_boilerplate(soup.get_text(separator='\n', strip=True))
            return "[실패] SBS Biz: API에 내용 없음"
        except Exception as e:
            return f"[오류] SBS Biz API: {e}"

    # 2. 스카이데일리 (HTML 조각 로딩 API)
    if 'skyedaily.com/news/news_view.html' in url:
        try:
            article_id_match = re.search(r'ID=(\d+)', url)
            if not article_id_match: return "[실패] 스카이데일리: 기사 ID 없음"
            article_id = article_id_match.group(1)
            
            initial_response = requests.get(url, headers=headers, timeout=10, verify=False)
            initial_response.raise_for_status()
            initial_response.encoding = 'euc-kr'
            initial_soup = BeautifulSoup(initial_response.text, 'lxml')
            content_area = initial_soup.select_one("#news_body_area")
            
            api_url = f"https://www.skyedaily.com/news/news_body_view_bottom.php?ID={article_id}"
            api_response = requests.get(api_url, timeout=10, verify=False, headers=headers)
            api_response.raise_for_status()
            api_response.encoding = 'euc-kr'
            api_soup = BeautifulSoup(api_response.text, 'lxml')
            
            if content_area:
                for tag in api_soup.contents:
                    content_area.append(tag)
                full_text = content_area.get_text(separator='\n', strip=True)
            else:
                full_text = api_soup.get_text(separator='\n', strip=True)
            
            return clean_bottom_boilerplate(full_text)
        except Exception as e:
            return f"[오류] 스카이데일리 API: {e}"

    # --- 전략 2: 강력한 범용 HTML 파서 (API가 없는 모든 사이트 대상) ---
    try:
        response = requests.get(url, headers=headers, timeout=15, verify=False)
        response.raise_for_status()

        if response.encoding.lower() == 'iso-8859-1':
            response.encoding = response.apparent_encoding

        soup = BeautifulSoup(response.text, "lxml")

        # 1단계: 불필요한 HTML 태그 사전 제거
        unwanted_tags = [
            'script', 'style', 'iframe', 'header', 'footer', 'nav', 'aside',
            '.ad', '.advertisement', '.banner', '.social-share', '.article_relation', 
            '.news_footer', '.journalist_area', '.copyright', '.comment', '.reply'
        ]
        for selector in unwanted_tags:
            for tag in soup.select(selector):
                tag.decompose()
        
        # 2단계: 알려진 선택자로 본문 영역 찾기 (신뢰도 높은 순서대로)
        selectors = [
            "#article-view-content-div", "#CmAdContent", "#articleBody", "#article-body",
            "#view_content_wrap", "#article-content-body", "#news_body_area", 
            "#article_content", "#news-contents", "#articleText",
            ".article_view_content_DIV", ".article_body", ".news_end", ".view-content", ".article-content",
            ".entry-content", ".td-post-content",
            "article"
        ]
        
        content_area = next((soup.select_one(s) for s in selectors if soup.select_one(s)), None)
        
        if content_area:
            text = content_area.get_text(separator='\n', strip=True)
            if len(text) > 150:
                return clean_bottom_boilerplate(text)

        # 3단계: 최후의 수단 (<p> 태그 조합)
        paragraphs = soup.find_all('p')
        if paragraphs:
            full_text, word_count = [], 0
            for p in paragraphs:
                p_text = p.get_text(strip=True)
                if len(p_text.split()) > 7: # 너무 짧은 문단은 제외
                    full_text.append(p_text)
                    word_count += len(p_text.split())
            
            if word_count > 100:
                return clean_bottom_boilerplate("\n\n".join(full_text))

        return "[실패] 모든 추출 방법 실패"

    except requests.exceptions.RequestException as e:
        return f"[오류] 네트워크: {e}"
    except Exception as e:
        return f"[오류] 일반 파싱: {e}"

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
    
    df = pd.DataFrame(new_articles)
    
    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce').dt.strftime('%Y-%m-%d')
    final_df = df[df['published_at'] >= thirty_days_ago].copy()
    
    for col in ['analysis_orgs', 'analysis_keywords']:
        if col in final_df.columns:
            final_df[col] = final_df[col].apply(lambda x: str(x) if isinstance(x, list) else str(x))

    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"aggregated_stock_data_{timestamp}.csv"
    csv_path = os.path.join(output_dir, csv_filename)
    final_df.to_csv(csv_path, index=False, encoding='utf-8-sig', quoting=csv.QUOTE_ALL)
    print(f"--- ✅ CSV 저장 완료. 총 {len(final_df)}개 기사 저장 ---")
    print(f"   - 저장 경로: {csv_path}")

# ==============================================================================
#  ▶️ Orchestrator: 메인 실행 함수
# ==============================================================================
def main():
    """전체 파이프라인을 순서대로 실행하는 메인 함수입니다."""
    print("="*50)
    print(" K-Stock News Analysis Pipeline (GitHub Actions) - START")
    print("="*50)
    
    try:
        initialize_gemini_model()
    except Exception as e:
        print(f"❌ 초기화 중 오류 발생: {e}")
        sys.exit(1)

    temp_existing_urls = set()
    new_articles = crawl_naver_news(STOCK_SEARCH_KEYWORDS, temp_existing_urls)
    
    if not new_articles:
        print("\n✅ 수집된 새로운 뉴스가 없습니다. 파이프라인을 종료합니다.")
        return
        
    print("\n--- 2단계: 기사 본문 추출 시작 ---")
    for i, article in enumerate(new_articles):
        # content가 이미 있는 경우는 건너뜁니다 (재실행 방지).
        if not article.get('content'):
            print(f"  - ({i+1}/{len(new_articles)}) 본문 추출 중: {article.get('url', '')[:70]}...")
            article['content'] = extract_article_content(article.get('url', ''))
            time.sleep(0.1) # 가벼운 딜레이
    print("--- ✅ 본문 추출 완료 ---")

    analyzed_articles = analyze_articles_with_ai(new_articles)
    
    output_dir = os.path.join("output", "aggregated")
    aggregate_and_save_to_csv(analyzed_articles, output_dir)

    print("\n" + "="*50)
    print(" K-Stock News Analysis Pipeline - COMPLETE")
    print("="*50)

if __name__ == "__main__":
    main()