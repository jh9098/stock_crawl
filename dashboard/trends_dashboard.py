
---

### 📁 2. `trends_dashboard.py` (데이터 시각화 대시보드)

이 파일은 Render Web Service나 Streamlit Community Cloud에 배포할 대시보드 코드입니다. JSONBin.io에서 데이터를 직접 읽어옵니다.
**(프로젝트의 `dashboard/` 폴더에 저장)**

```python
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
import requests
import json

# =========================== 기본 설정 ===========================
st.set_page_config(
    page_title="뉴스 트렌드 분석 대시보드",
    page_icon="📈",
    layout="wide",
)

# --- JSONBin.io 설정 ---
# 🚨 아래 값들은 배포 플랫폼의 Secret으로 설정하는 것이 가장 안전합니다.
# 로컬 테스트 시에는 직접 입력하거나 환경 변수로 설정하세요.
JSONBIN_API_KEY = os.getenv("JSONBIN_API_KEY", "여기에_JSONBIN_API_KEY를_입력") 
JSONBIN_BIN_ID = os.getenv("JSONBIN_BIN_ID", "여기에_JSONBIN_BIN_ID를_입력")

# ----- 디폴트 값 -----
DEFAULT_TOP_N_KEY_ORG = 15
DEFAULT_RECENT_DAYS   = 7
DEFAULT_PREV_DAYS     = 7
EPS = 1e-6

STOP_KEYWORDS = {
    "한국", "정부", "정책", "발표", "관련", "시장", "증시", "경제", "주식", "이날",
    "기사", "분석", "업계", "회사", "금융", "투자", "실적"
}

# =========================== 데이터 로딩 함수 ===========================
def safe_literal_eval(val):
    try:
        return ast.literal_eval(str(val))
    except (ValueError, SyntaxError, TypeError):
        return []

@st.cache_data(ttl=600) # 10분마다 데이터 새로고침
def load_data_from_bin(api_key, bin_id):
    """JSONBin에서 최신 데이터를 로드하고 전처리합니다."""
    if "여기에" in api_key or "여기에" in bin_id:
        st.error("JSONBin.io API 키와 Bin ID를 설정해야 합니다.")
        return None

    headers = {'X-Master-Key': api_key, 'X-Bin-Versioning': 'false'}
    url = f"https://api.jsonbin.io/v3/b/{bin_id}/latest"
    try:
        req = requests.get(url, headers=headers, timeout=15)
        req.raise_for_status()
        data = req.json()
        
        df = pd.DataFrame(data)

        # 날짜처리
        df['analysis_date'] = pd.to_datetime(df['published_at'], errors='coerce').dt.date
        df.dropna(subset=['analysis_date'], inplace=True)

        # 리스트 컬럼 처리
        df['analysis_keywords'] = df['analysis_keywords'].apply(safe_literal_eval)
        df['analysis_orgs']     = df['analysis_orgs'].apply(safe_literal_eval)

        return df
    except Exception as e:
        st.error(f"원격 데이터 로딩 중 오류 발생: {e}")
        return None

# =========================== 데이터 로드 ===========================
df = load_data_from_bin(JSONBIN_API_KEY, JSONBIN_BIN_ID)

st.title("📈 뉴스 트렌드 분석 대시보드 (자동 업데이트)")

if df is None or df.empty:
    st.warning("데이터를 불러오지 못했습니다. 잠시 후 다시 시도해주세요.")
    st.stop()
    
# (이하 대시보드 UI 코드는 기존 `trends_dashboard.py`와 거의 동일하게 사용 가능)
# (경로 설정 및 일부 로직만 원격 데이터에 맞게 수정됨)

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
st.sidebar.subheader("🔤 키워드/기관 표시 개수")
TOP_N_KEY_ORG = st.sidebar.number_input("상위 개수", 5, 50, DEFAULT_TOP_N_KEY_ORG, 1)

st.sidebar.markdown("---")
st.sidebar.subheader("🔥 모멘텀 분석 설정")
recent_days = st.sidebar.number_input("최근 N일", 3, 30, DEFAULT_RECENT_DAYS, 1)
prev_days   = st.sidebar.number_input("직전 N일", 3, 30, DEFAULT_PREV_DAYS, 1)

# =========================== 데이터 요약 ===========================
st.success(f"총 **{len(filtered_df)}개 기사** 분석 (기간: {start_date} ~ {end_date})")

# =========================== 키워드/기관 시계열 ===========================
col1, col2 = st.columns(2)

with col1:
    st.subheader("🗓️ 주요 키워드 언급량")
    exploded_kw = filtered_df.explode('analysis_keywords').dropna(subset=['analysis_keywords'])
    exploded_kw = exploded_kw[~exploded_kw['analysis_keywords'].isin(STOP_KEYWORDS)]
    if not exploded_kw.empty:
        daily_counts_kw = exploded_kw.groupby(['analysis_date', 'analysis_keywords']).size().reset_index(name='count')
        top_kw = daily_counts_kw.groupby('analysis_keywords')['count'].sum().nlargest(TOP_N_KEY_ORG).index
        top_kw_df = daily_counts_kw[daily_counts_kw['analysis_keywords'].isin(top_kw)]
        if not top_kw_df.empty:
            fig_kw = px.line(top_kw_df, x='analysis_date', y='count', color='analysis_keywords', markers=True)
            st.plotly_chart(fig_kw, use_container_width=True)

with col2:
    st.subheader("🏢 주요 기관/기업 언급량")
    exploded_org = filtered_df.explode('analysis_orgs').dropna(subset=['analysis_orgs'])
    if not exploded_org.empty:
        daily_counts_org = exploded_org.groupby(['analysis_date', 'analysis_orgs']).size().reset_index(name='count')
        top_org = daily_counts_org.groupby('analysis_orgs')['count'].sum().nlargest(TOP_N_KEY_ORG).index
        top_org_df = daily_counts_org[daily_counts_org['analysis_orgs'].isin(top_org)]
        if not top_org_df.empty:
            fig_org = px.line(top_org_df, x='analysis_date', y='count', color='analysis_orgs', markers=True)
            st.plotly_chart(fig_org, use_container_width=True)

# (이하 모멘텀 분석 등 다른 차트들도 동일한 방식으로 구현 가능)