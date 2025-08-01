# dashboard/trends_dashboard.py
# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import csv
import numpy as np
import ast
from pathlib import Path
from math import log
import requests # requests 라이브러리 임포트 확인

# =========================== 기본 설정 ===========================
st.set_page_config(
    page_title="뉴스 트렌드 분석 대시보드",
    page_icon="📈",
    layout="wide",
)

# --- [핵심 수정] GitHub Raw URL에서 데이터 로드 ---
# 🚨 아래 URL의 'YourUsername/YourRepoName' 부분을 
#    본인의 실제 GitHub 사용자명과 저장소 이름으로 반드시 바꿔주세요!
# 예시: "https://raw.githubusercontent.com/jh9098/stock_crawl/main/backend/output/aggregated/aggregated_stock_data.csv"
DATA_URL = "https://raw.githubusercontent.com/jh9098/stock_crawl/main/backend/output/aggregated/aggregated_stock_data.csv"

# ----- 경로 설정 (코스피/코스닥 파일용) -----
# 이 파일(trends_dashboard.py)이 있는 위치를 기준으로 경로를 잡습니다.
DASHBOARD_ROOT = os.path.dirname(os.path.abspath(__file__))
KOSPI_TXT   = os.path.join(DASHBOARD_ROOT, "코스피.txt")
KOSDAQ_TXT  = os.path.join(DASHBOARD_ROOT, "코스닥.txt")

# ----- 디폴트 값 -----
DEFAULT_TOP_N_KEY_ORG = 15
DEFAULT_TOP_N_STOCKS  = 15
DEFAULT_RECENT_DAYS   = 7
DEFAULT_PREV_DAYS     = 7
EPS = 1e-6

STOP_KEYWORDS = {
    "한국", "정부", "정책", "발표", "관련", "시장", "증시", "경제", "주식", "이날",
    "기사", "분석", "업계", "회사", "금융", "투자", "실적"
}

# =========================== 유틸 함수 ===========================
def safe_literal_eval(val):
    """CSV에서 읽은 리스트 형태의 문자열 -> 실제 리스트로 변환"""
    try:
        # nan 같은 float 타입이 들어올 경우를 대비해 문자열로 먼저 변환
        return ast.literal_eval(str(val))
    except (ValueError, SyntaxError, TypeError):
        return []

@st.cache_data(ttl=600)
def load_data_from_local(file_path):
    try:
        df = pd.read_csv(file_path, encoding='utf-8')
        # 날짜 처리
        published_dt = pd.to_datetime(df['published_at'], errors='coerce')
        crawled_dt = pd.to_datetime(df['crawled_at'], errors='coerce') if 'crawled_at' in df.columns else None
        df['analysis_date'] = np.where(pd.notna(published_dt), published_dt, crawled_dt)
        df['analysis_date'] = pd.to_datetime(df['analysis_date']).dt.date
        df.dropna(subset=['analysis_date'], inplace=True)
        # 리스트 컬럼 처리
        df['analysis_keywords'] = df['analysis_keywords'].apply(safe_literal_eval)
        df['analysis_orgs'] = df['analysis_orgs'].apply(safe_literal_eval)
        return df
    except Exception as e:
        st.error(f"로컬 파일 데이터 로딩 중 오류 발생: {e}")
        st.info("파일 경로와 인코딩을 확인해주세요.")
        return None

# 실제 파일 경로 입력 (여기만 수정!)
LOCAL_CSV_PATH = r"P:\stock_crawl\backend\output\merged_no_duplicate.csv"
df = load_data_from_local(LOCAL_CSV_PATH)


@st.cache_data
def load_stock_names(kospi_path: str, kosdaq_path: str):
    """코스피/코스닥 텍스트 파일에서 종목명 리스트 로드"""
    names = []
    for p in [kospi_path, kosdaq_path]:
        try:
            # 웹 배포 환경을 고려하여 Path 사용
            txt_path = Path(p)
            if txt_path.is_file():
                txt = txt_path.read_text(encoding='utf-8').splitlines()
                names.extend([x.strip() for x in txt if x.strip()])
            else:
                 st.warning(f"종목 리스트 파일을 찾지 못했습니다: {p}")
        except Exception as e:
            st.warning(f"종목 리스트 로딩 중 오류 ({p}): {e}")
    return sorted(set(names))

def extract_stock_mentions(org_list, stock_set):
    """분석된 기관/기업 리스트 중 종목명만 추출"""
    if not isinstance(org_list, list):
        return []
    # analysis_orgs에 있는 이름이 stock_set에 포함되는 경우만 필터링
    return [org for org in org_list if org in stock_set]


# =========================== 데이터 로드 ===========================
df = load_data_from_local(LOCAL_CSV_PATH)
stock_list = load_stock_names(KOSPI_TXT, KOSDAQ_TXT)
stock_set  = set(stock_list)

st.title("📈 뉴스 트렌드 분석 대시보드 (자동 업데이트)")

if df is None or df.empty:
    st.warning("데이터를 불러오지 못했습니다. GitHub Actions가 아직 실행되지 않았거나, 데이터 로딩에 실패했습니다.")
    st.stop()

# =========================== 사이드바 필터 ===========================
st.sidebar.header("📊 기본 필터")

min_date = df['analysis_date'].min()
max_date = df['analysis_date'].max()

date_range = st.sidebar.date_input(
    "날짜 범위 선택",
    value=(min_date, max_date), min_value=min_date, max_value=max_date
)

if len(date_range) != 2:
    st.stop()

start_date, end_date = date_range
filtered_df = df[(df['analysis_date'] >= start_date) & (df['analysis_date'] <= end_date)].copy()

st.sidebar.markdown("---")
st.sidebar.subheader("🔤 표시 개수 설정")
TOP_N_KEY_ORG = st.sidebar.number_input("키워드/기관 상위 개수", 5, 50, DEFAULT_TOP_N_KEY_ORG, 1)
TOP_N_STOCKS = st.sidebar.number_input("시계열 상위 종목 개수", 5, 50, DEFAULT_TOP_N_STOCKS, 1)

st.sidebar.markdown("---")
st.sidebar.subheader("🔥 모멘텀 분석 설정")
recent_days = st.sidebar.number_input("최근 N일", 3, 30, DEFAULT_RECENT_DAYS, 1)
prev_days   = st.sidebar.number_input("직전 N일", 3, 30, DEFAULT_PREV_DAYS, 1)

# ... (기존 코드 동일, 위 생략) ...

# ===================== 데이터 요약 및 기본 준비 =====================
st.success(f"총 **{len(filtered_df)}개 기사** 분석 (기간: {start_date} ~ {end_date})")

if not stock_set:
    st.warning("코스피/코스닥 종목 리스트가 비어 있습니다. txt 파일을 확인해주세요.")
else:
    filtered_df['stock_mentions'] = filtered_df['analysis_orgs'].apply(lambda orgs: extract_stock_mentions(orgs, stock_set))

# ===================== 1. 시계열 트렌드(키워드/기관/종목) =====================
st.markdown("---")
st.header("📊 시계열 트렌드 분석")

st.subheader("🗓️ 주요 키워드")
exploded_kw = filtered_df.explode('analysis_keywords').dropna(subset=['analysis_keywords'])
exploded_kw = exploded_kw[~exploded_kw['analysis_keywords'].isin(STOP_KEYWORDS | stock_set)]
if not exploded_kw.empty:
    daily_counts_kw = exploded_kw.groupby(['analysis_date', 'analysis_keywords']).size().reset_index(name='count')
    top_kw = daily_counts_kw.groupby('analysis_keywords')['count'].sum().nlargest(TOP_N_KEY_ORG).index
    top_kw_df = daily_counts_kw[daily_counts_kw['analysis_keywords'].isin(top_kw)]
    if not top_kw_df.empty:
        fig_kw = px.line(top_kw_df, x='analysis_date', y='count', color='analysis_keywords', markers=True, title=f"상위 {TOP_N_KEY_ORG} 키워드 언급 추이")
        st.plotly_chart(fig_kw, use_container_width=True)

st.subheader("🏢 주요 기관 (Non-stock)")
exploded_org = filtered_df.explode('analysis_orgs').dropna(subset=['analysis_orgs'])
exploded_org = exploded_org[~exploded_org['analysis_orgs'].isin(stock_set)]
if not exploded_org.empty:
    daily_counts_org = exploded_org.groupby(['analysis_date', 'analysis_orgs']).size().reset_index(name='count')
    top_org = daily_counts_org.groupby('analysis_orgs')['count'].sum().nlargest(TOP_N_KEY_ORG).index
    top_org_df = daily_counts_org[daily_counts_org['analysis_orgs'].isin(top_org)]
    if not top_org_df.empty:
        fig_org = px.line(top_org_df, x='analysis_date', y='count', color='analysis_orgs', markers=True, title=f"상위 {TOP_N_KEY_ORG} 기관 언급 추이")
        st.plotly_chart(fig_org, use_container_width=True)

st.subheader("📈 주요 종목")
exploded_stock = filtered_df.explode('stock_mentions').dropna(subset=['stock_mentions'])
if not exploded_stock.empty:
    daily_counts_stock = exploded_stock.groupby(['analysis_date', 'stock_mentions']).size().reset_index(name='count')
    top_stock = daily_counts_stock.groupby('stock_mentions')['count'].sum().nlargest(TOP_N_STOCKS).index
    top_stock_df = daily_counts_stock[daily_counts_stock['stock_mentions'].isin(top_stock)]
    if not top_stock_df.empty:
        fig_stock = px.line(top_stock_df, x='analysis_date', y='count', color='stock_mentions', markers=True, title=f"상위 {TOP_N_STOCKS} 종목 언급 추이")
        st.plotly_chart(fig_stock, use_container_width=True)

# ===================== 2. 급상승(트렌딩) 키워드/종목 분석 =====================
st.markdown("---")
st.header("🚀 최근 급상승 키워드/종목/이슈 분석")

import collections
from datetime import datetime, timedelta

def flatten_keywords(series):
    # 문자열 리스트 → 진짜 리스트로 평탄화
    result = []
    for x in series:
        try:
            result.extend([k for k in ast.literal_eval(str(x)) if isinstance(k, str)])
        except Exception:
            continue
    return result

now = pd.Timestamp.now()
recent_limit = now - pd.Timedelta(days=recent_days)
prev_limit = recent_limit - pd.Timedelta(days=prev_days)

recent_df = filtered_df[filtered_df['analysis_date'] >= recent_limit.date()]
prev_df   = filtered_df[(filtered_df['analysis_date'] < recent_limit.date()) & (filtered_df['analysis_date'] >= prev_limit.date())]

recent_kw = collections.Counter(flatten_keywords(recent_df['analysis_keywords']))
prev_kw   = collections.Counter(flatten_keywords(prev_df['analysis_keywords']))

trending = []
for k in recent_kw:
    prev_count = prev_kw.get(k, 0)
    if recent_kw[k] >= 3 and recent_kw[k] > prev_count:  # 최소 등장 횟수 필터
        rate = ((recent_kw[k]-prev_count)/prev_count*100) if prev_count else 1000  # 0 대비는 1000%
        trending.append((k, recent_kw[k], prev_count, rate))
trending.sort(key=lambda x: x[3], reverse=True)
trend_df = pd.DataFrame(trending, columns=["키워드", f"최근 {recent_days}일", f"직전 {prev_days}일", "증가율(%)"])
st.subheader(f"🔥 최근 {recent_days}일 급상승 키워드 Top 10")
st.dataframe(trend_df.head(10))

# ===================== 3. 종목별 감성(긍/부/중) 시계열 =====================
st.markdown("---")
st.header("😃 종목별 감성 추이 (긍/부/중 시계열)")

sentiment_df = filtered_df.explode('stock_mentions').dropna(subset=['stock_mentions'])
sentiment_ts = sentiment_df.groupby(['analysis_date', 'stock_mentions', 'sentiment_label']).size().reset_index(name='count')
top_stock = sentiment_ts.groupby('stock_mentions')['count'].sum().nlargest(TOP_N_STOCKS).index
sentiment_ts = sentiment_ts[sentiment_ts['stock_mentions'].isin(top_stock)]

if not sentiment_ts.empty:
    fig = px.line(
        sentiment_ts,
        x='analysis_date', y='count',
        color='sentiment_label',
        facet_row='stock_mentions',
        markers=True,
        title='상위 종목별 감성 추이'
    )
    st.plotly_chart(fig, use_container_width=True)

# ===================== 4. 감성분석 비율 (긍/부/중) 전체 요약 =====================
st.markdown("---")
st.header("🧠 전체 감성 분포 (긍/부/중)")

sent_count = filtered_df['sentiment_label'].value_counts()
st.write("기사 전체 감성 분포 (건수 기준):")
st.bar_chart(sent_count)

# ===================== 5. 최근 테마/이슈 클러스터 및 요약(기초) =====================
st.markdown("---")
st.header("🔎 최근 테마별 기사 집계 (기초 클러스터링)")

theme_df = exploded_kw.groupby(['analysis_keywords'])['url'].count().sort_values(ascending=False).reset_index()
theme_df = theme_df[~theme_df['analysis_keywords'].isin(STOP_KEYWORDS | stock_set)]
st.write("최근 기사에서 가장 많이 등장한 이슈/테마 Top 20")
st.dataframe(theme_df.head(20))

# ===================== 6. 정책/제도/리스크 이슈 뉴스 필터 =====================
st.markdown("---")
st.header("⚠️ 정책/제도/리스크 관련 뉴스")

policy_words = {"정책", "규제", "법안", "세제", "금리", "정부", "당국", "공시", "발표", "리스크", "위기"}
policy_mask = exploded_kw['analysis_keywords'].apply(lambda x: any(pw in str(x) for pw in policy_words))
policy_news = exploded_kw[policy_mask][['analysis_date', 'url', 'analysis_keywords']].drop_duplicates().head(50)
st.write("정책/제도/리스크 관련 최근 뉴스 Top 50")
st.dataframe(policy_news)

# ===================== 7. 기사별 요약·AI 코멘트/투자포인트 =====================
st.markdown("---")
st.header("💡 AI 기사 요약/투자포인트/한줄평")

ai_summary = filtered_df[['analysis_date', 'url', 'summary_ai', 'sentiment_label']].sort_values('analysis_date', ascending=False).head(20)
st.write("최신 기사 한줄 요약/AI 코멘트 (최신순 Top 20)")
st.dataframe(ai_summary)

# ===================== 8. 유사뉴스/연관 이슈 추천 (기초, 키워드 기반) =====================
st.markdown("---")
st.header("🔗 연관/유사 뉴스 추천 (키워드 기반)")

def find_related_news(keyword, df, topn=10):
    # 입력 키워드와 연관된 기사 최신순 추천
    mask = df['analysis_keywords'].apply(lambda kws: keyword in kws if isinstance(kws, list) else False)
    return df[mask][['analysis_date', 'url', 'summary_ai', 'sentiment_label']].sort_values('analysis_date', ascending=False).head(topn)

user_kw = st.text_input("연관 뉴스 검색할 키워드 입력(예: '2차전지', 'AI반도체')")
if user_kw:
    st.write(f"'{user_kw}' 관련 최신 뉴스 Top 10:")
    st.dataframe(find_related_news(user_kw, filtered_df))

# ===================== 끝 =====================
