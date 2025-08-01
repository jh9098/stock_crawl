# backend/run_ai_only.py
# -*- coding: utf-8 -*-
# 목적: 이미 수집된 CSV 파일을 읽어 AI 분석만 수행하는 임시 스크립트

import os
import sys
import json
import pandas as pd
import google.generativeai as genai
from datetime import datetime, timedelta

# --- 원본 스크립트에서 AI 분석에 필요한 함수만 가져옴 ---

# 설정 영역 (일부만 필요)
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
BATCH_SIZE = 5

gemini_model = None

def initialize_gemini_model():
    """Gemini 모델을 초기화합니다."""
    global gemini_model
    if not GOOGLE_API_KEY:
        raise ValueError("Google API 키가 GitHub Secrets에 설정되지 않았습니다.")
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
]"""
def analyze_articles_with_ai(articles):
    """기사 목록을 AI를 통해 분석하고 구조화된 데이터를 추가합니다."""
    print("\n--- AI 구조화 분석 시작 ---")
    articles_to_process, article_map = [], {}
    for i, article in enumerate(articles):
        article['unique_id'] = f"art_{i}"
        article_map[article['unique_id']] = article
        # CSV에서 읽어온 content가 비어있는 경우(NaN)를 대비해 문자열로 변환
        content_to_analyze = str(article.get("content", ""))
        if not content_to_analyze or content_to_analyze.startswith(("[실패]", "[오류]")):
            content_to_analyze = str(article.get("summary", ""))
        if content_to_analyze:
            article['content_to_analyze'] = content_to_analyze
            articles_to_process.append(article)
    print(f"  - AI 분석 대상: {len(articles_to_process)}개 / 총 {len(articles)}개")
    if not articles_to_process:
        return articles

    total_batches = (len(articles_to_process) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(0, len(articles_to_process), BATCH_SIZE):
        batch = articles_to_process[i:i + BATCH_SIZE]
        current_batch_num = i // BATCH_SIZE + 1
        print(f"  - 배치 {current_batch_num}/{total_batches} 처리 중... ({len(batch)}개)")
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
                print(f"    ⚠️ 배치 {current_batch_num} 분석 결과가 리스트가 아님.")
        except Exception as e:
            print(f"    - 배치 {current_batch_num} 분석 중 오류: {e}")

    final_list = list(article_map.values())
    for art in final_list:
        art.pop('unique_id', None)
        art.pop('id', None)
        art.pop('content_to_analyze', None)
    print("--- ✅ AI 분석 완료 ---")
    return final_list

def aggregate_and_save_to_csv(new_articles, output_dir):
    """분석 완료된 기사를 최종 CSV 파일로 저장합니다."""
    print("\n--- 최종 데이터 저장 시작 ---")
    if not new_articles:
        print(" - 저장할 새 데이터가 없습니다.")
        return
    df = pd.DataFrame(new_articles)

    # CSV에 저장하기 좋게 리스트 형태의 컬럼을 문자열로 변환
    for col in ['analysis_orgs', 'analysis_keywords']:
        if col in df.columns:
            # NaN 값이 있을 경우를 대비해 비어있는 문자열 리스트('[]')로 변환
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else '[]')

    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, "aggregated_stock_data.csv")
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"--- ✅ CSV 저장 완료. 총 {len(df)}개 기사 저장 ---")
    print(f"   - 저장 경로: {csv_path}")

def main():
    """
    저장된 CSV 파일을 읽어 AI 분석만 수행하고 결과를 저장합니다.
    """
    print("="*50)
    print(" K-Stock News AI Analysis Only - START")
    print("="*50)
    # 본문 추출까지 완료된 입력 파일 경로
    input_csv_path = "backend/output/intermediate/crawled_data.csv"
    # 최종 결과물이 저장될 폴더 경로
    output_dir = os.path.join("backend", "output", "aggregated")

    # 1. AI 모델 초기화
    try:
        initialize_gemini_model()
    except Exception as e:
        print(f"❌ 초기화 중 오류 발생: {e}")
        sys.exit(1)

    # 2. CSV 파일 읽기
    print(f"\n--- 데이터 로드 시작: {input_csv_path} ---")
    if not os.path.exists(input_csv_path):
        print(f"🚨 입력 파일({input_csv_path})을 찾을 수 없습니다! 스크립트를 종료합니다.")
        sys.exit(1)

    df = pd.read_csv(input_csv_path)
    # pandas가 CSV를 읽을 때 빈 셀을 NaN으로 읽는 경우가 있으므로, 이를 빈 문자열로 대체
    df = df.fillna('') 
    articles_to_analyze = df.to_dict('records')
    print(f"✅ {len(articles_to_analyze)}개의 기사를 파일에서 로드했습니다.")

    # 3. AI 분석 실행
    analyzed_articles = analyze_articles_with_ai(articles_to_analyze)

    # 4. 최종 결과 저장
    aggregate_and_save_to_csv(analyzed_articles, output_dir)

    print("\n" + "="*50)
    print(" K-Stock News AI Analysis Only - COMPLETE")
    print("="*50)

if __name__ == "__main__":
    main()
