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

@st.cache_data(ttl=600) # 10분마다 GitHub에서 데이터 새로고침
def load_data_from_github(url):
    """GitHub Raw URL에서 최신 CSV 데이터를 로드하고 전처리합니다."""
    try:
        df = pd.read_csv(url)
        
        # 날짜처리
        published_dt = pd.to_datetime(df['published_at'], errors='coerce')
        crawled_dt   = pd.to_datetime(df['crawled_at'],   errors='coerce')
        df['analysis_date'] = np.where(pd.notna(published_dt), published_dt, crawled_dt)
        df['analysis_date'] = pd.to_datetime(df['analysis_date']).dt.date
        df.dropna(subset=['analysis_date'], inplace=True)

        # 리스트 컬럼 처리
        df['analysis_keywords'] = df['analysis_keywords'].apply(safe_literal_eval)
        df['analysis_orgs']     = df['analysis_orgs'].apply(safe_literal_eval)

        return df
    except Exception as e:
        st.error(f"GitHub에서 데이터 로딩 중 오류 발생: {e}")
        st.info("데이터 URL이 정확한지, 그리고 GitHub 저장소의 해당 경로에 CSV 파일이 생성되었는지 확인해주세요.")
        return None

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
df = load_data_from_github(DATA_URL)
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

# =========================== 데이터 요약 ===========================
st.success(f"총 **{len(filtered_df)}개 기사** 분석 (기간: {start_date} ~ {end_date})")

# =========================== 종목 분석 공통 준비 ===========================
if not stock_set:
    st.warning("코스피/코스닥 종목 리스트가 비어 있습니다. txt 파일을 확인해주세요.")
else:
    # analysis_orgs 에서 주식 종목만 필터링하여 'stock_mentions' 컬럼 생성
    filtered_df['stock_mentions'] = filtered_df['analysis_orgs'].apply(lambda orgs: extract_stock_mentions(orgs, stock_set))

# =========================== 키워드/기관/종목 시계열 ===========================
st.markdown("---")
st.header("📊 시계열 트렌드 분석")
col1, col2, col3 = st.columns(3)

with col1:
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

with col2:
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

with col3:
    st.subheader("📈 주요 종목")
    exploded_stock = filtered_df.explode('stock_mentions').dropna(subset=['stock_mentions'])
    if not exploded_stock.empty:
        daily_counts_stock = exploded_stock.groupby(['analysis_date', 'stock_mentions']).size().reset_index(name='count')
        top_stock = daily_counts_stock.groupby('stock_mentions')['count'].sum().nlargest(TOP_N_STOCKS).index
        top_stock_df = daily_counts_stock[daily_counts_stock['stock_mentions'].isin(top_stock)]
        if not top_stock_df.empty:
            fig_stock = px.line(top_stock_df, x='analysis_date', y='count', color='stock_mentions', markers=True, title=f"상위 {TOP_N_STOCKS} 종목 언급 추이")
            st.plotly_chart(fig_stock, use_container_width=True)

# (이하 모멘텀 분석 등 다른 기능들도 위와 유사한 방식으로 구현 가능)