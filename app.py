import streamlit as st
import sqlite3
import pandas as pd
from pykrx import stock
from datetime import datetime, timedelta
import re
import requests
import json
from urllib.parse import quote
import os
from dotenv import load_dotenv
import io
import time
import plotly.express as px
import plotly.graph_objects as go
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --- 환경 변수 설정 ---
load_dotenv()

# Streamlit Cloud secrets 우선, 없으면 .env 환경변수 사용
NAVER_CLIENT_ID = st.secrets.get("NAVER_CLIENT_ID", os.getenv("NAVER_CLIENT_ID"))
NAVER_CLIENT_SECRET = st.secrets.get("NAVER_CLIENT_SECRET", os.getenv("NAVER_CLIENT_SECRET"))
GOOGLE_SHEET_PASSWORD = st.secrets.get("GOOGLE_SHEET_PASSWORD", os.getenv("GOOGLE_SHEET_PASSWORD", "default_password"))
SPREADSHEET_ID = st.secrets.get("GOOGLE_SPREADSHEET_ID", os.getenv("GOOGLE_SPREADSHEET_ID"))

st.set_page_config(
    page_title="급등주 탐지기 Pro",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS를 사용하여 기본 여백 조정 및 최대 너비 설정
st.markdown("""
<style>
    .block-container {
        padding-top: 1rem;
        padding-bottom: 0rem;
        padding-left: 10rem;
        padding-right: 10rem;
        max-width: 100%;
    }
    .element-container {
        width: 100%;
        padding-left: 0;
        padding-right: 0;
    }
    .stDataFrame {
        width: 100%;
        padding-left: 0;
        padding-right: 0;
    }
    div[data-testid="stToolbar"] {
        display: none;
    }
    #MainMenu {
        visibility: hidden;
    }
    div[data-testid="stDecoration"] {
        display: none;
    }
    div[data-testid="stHeader"] {
        display: none;
    }
    h1 {
        font-size: 2.5rem !important;
        margin-bottom: 0.5rem !important;
        padding-left: 0.5rem;
    }
    h2 {
        font-size: 1.8rem !important;
        margin-top: 1rem !important;
        margin-bottom: 0.5rem !important;
        padding-left: 0.5rem;
    }
    h3 {
        font-size: 1.4rem !important;
        margin-top: 0.8rem !important;
        margin-bottom: 0.4rem !important;
        padding-left: 0.5rem;
    }
    h4 {
        font-size: 1.2rem !important;
        margin-top: 0.6rem !important;
        margin-bottom: 0.3rem !important;
        padding-left: 0.5rem;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        padding-left: 0.5rem;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 0.5rem 1rem;
        margin: 0;
        font-size: 1.25rem !important;
        font-weight: bold !important;
        padding-top: 0.7rem !important;
        padding-bottom: 0.7rem !important;
    }
    .stTabs [data-baseweb="tab-panel"] {
        padding: 0.5rem 0;
    }
    .stDataFrame > div {
        padding-left: 0;
        padding-right: 0;
    }
    .stDataFrame > div > div {
        padding-left: 0;
        padding-right: 0;
    }
    .stDataFrame > div > div > div {
        padding-left: 0;
        padding-right: 0;
    }
    /* 테이블 셀 내부 여백 조정 */
    .stDataFrame td, .stDataFrame th {
        padding: 0.3rem 0.5rem !important; /* 상하 0.3rem, 좌우 0.5rem 패딩 */
    }
</style>
""", unsafe_allow_html=True)

# 빈 공간 추가
st.markdown("<br>", unsafe_allow_html=True)

# 앱 제목
st.markdown("""
    <h1 style='text-align: center;'>급등주 탐지기 Pro</h1>
""", unsafe_allow_html=True)

def get_google_sheet():
    SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    CREDENTIALS_FILE = 'credentials.json'
    SPREADSHEET_ID = st.secrets.get("GOOGLE_SPREADSHEET_ID", os.getenv("GOOGLE_SPREADSHEET_ID"))
    try:
        if not os.path.exists(CREDENTIALS_FILE):
            st.error(f"오류: {CREDENTIALS_FILE} 파일이 존재하지 않습니다.")
            return None
        if not SPREADSHEET_ID:
            st.error("오류: GOOGLE_SPREADSHEET_ID 환경 변수가 설정되지 않았습니다.")
            return None
        credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, SCOPES)
        client = gspread.authorize(credentials)
        sheet = client.open_by_key(SPREADSHEET_ID)
        return sheet
    except Exception as e:
        st.error(f"구글 시트 연결 중 오류 발생: {e}")
        return None

def update_google_sheet(data_df, date_str):
    if data_df is None or data_df.empty:
        return False, "업데이트할 데이터가 없습니다."
    try:
        sheet = get_google_sheet()
        if not sheet:
            return False, "구글 시트 연결 실패"
        year_month = f"{date_str[:4]}-{date_str[4:6]}"
        worksheet_name = f"{year_month}"
        try:
            worksheet = sheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sheet.add_worksheet(title=worksheet_name, rows=2000, cols=30)
        worksheet.clear()
        worksheet.update([list(data_df.columns)], f'A1')
        worksheet.update(data_df.values.tolist(), f'A2')
        return True, f"구글 시트 업데이트 완료 (추가된 데이터: {len(data_df)}개)"
    except Exception as e:
        return False, f"구글 시트 업데이트 실패: {str(e)}"
    
def load_company_info_from_krx_url(krx_url, column_names_map):
    """KRX에서 제공하는 URL로부터 상장법인목록 데이터를 HTML 테이블 형식으로 로드합니다."""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(krx_url, headers=headers, timeout=30)
        response.raise_for_status()
        
        df_company_info = None
        
        try:
            try:
                dfs = pd.read_html(io.StringIO(response.text), header=0)
            except UnicodeDecodeError:
                dfs = pd.read_html(io.StringIO(response.content.decode('cp949')), header=0)
            except Exception as e_decode_generic:
                dfs = pd.read_html(io.StringIO(response.content.decode('utf-8', errors='replace')), header=0)

            if not dfs:
                st.warning("HTML에서 테이블을 찾지 못했습니다.")
                return None
            df_company_info = dfs[0]
            
            ticker_col_name_in_html = column_names_map.get('source_ticker_col', '종목코드')
            if ticker_col_name_in_html in df_company_info.columns:
                 df_company_info[ticker_col_name_in_html] = df_company_info[ticker_col_name_in_html].astype(str)
        except Exception as e_html:
            st.error(f"HTML 형식으로 파싱 실패: {e_html}")
            return None
        
        if df_company_info is None or df_company_info.empty:
            st.warning("데이터를 성공적으로 파싱하지 못했습니다.")
            return None

        required_source_cols = {
            '티커': column_names_map.get('source_ticker_col', '종목코드'),
            '업종': column_names_map.get('source_industry_col', '업종'),
            '주요제품': column_names_map.get('source_products_col', '주요제품')
        }

        cols_to_use = {} 
        for standard_col, source_col_name in required_source_cols.items():
            if source_col_name in df_company_info.columns:
                cols_to_use[source_col_name] = standard_col
            else:
                st.warning(f"원본 데이터에 '{source_col_name}' 컬럼이 없습니다. '{standard_col}' 정보는 비어있게 됩니다.")
        
        if '티커' not in cols_to_use.values():
             st.error(f"원본 데이터에서 필수 컬럼인 '{required_source_cols['티커']}'을(를) 찾을 수 없습니다.")
             return None

        df_selected_info = df_company_info[list(cols_to_use.keys())].copy()
        df_selected_info.rename(columns=cols_to_use, inplace=True)
        
        for standard_col in ['티커', '업종', '주요제품']:
            if standard_col not in df_selected_info.columns:
                df_selected_info[standard_col] = ""
        
        if '티커' in df_selected_info.columns:
             df_selected_info['티커'] = df_selected_info['티커'].astype(str).str.strip().str.zfill(6)

        return df_selected_info[['티커', '업종', '주요제품']]

    except requests.exceptions.RequestException as e_req:
        st.error(f"KRX 기업 정보 다운로드 중 오류 발생: {e_req}")
        return None
    except Exception as e_general:
        st.error(f"KRX 정보 처리 중 예상치 못한 오류 발생: {e_general}")
        return None

# --- 기존 스크립트의 헬퍼 함수들 ---
OUTPUT_COLUMNS_WITH_REMARKS = [
    '날짜', '티커', '종목명', '업종', '주요제품', '시가', '고가', '저가', '종가', '등락률', '거래량', '거래대금', '시장', '비고',
    '기사제목1', '기사요약1', '기사링크1',
    '기사제목2', '기사요약2', '기사링크2',
    '기사제목3', '기사요약3', '기사링크3',
    '기사제목4', '기사요약4', '기사링크4',
    '기사제목5', '기사요약5', '기사링크5'
]

def call_naver_search_api(query, display_count, client_id, client_secret):
    """네이버 뉴스 API를 호출하여 뉴스 데이터를 가져옵니다."""
    if not client_id or not client_secret:
        st.error("오류: Naver API Client ID 또는 Client Secret이 제공되지 않았습니다.")
        return []

    encoded_query = quote(query)
    all_processed_items = []
    max_api_display_per_call = 100
    max_start_value = 1000
    num_calls_needed = min((display_count + max_api_display_per_call - 1) // max_api_display_per_call, 
                          (max_start_value + max_api_display_per_call - 1) // max_api_display_per_call)
    
    for i in range(num_calls_needed):
        try:
            start_index = 1 + (i * max_api_display_per_call)
            if start_index > max_start_value:
                break
                
            current_display_needed = min(max_api_display_per_call, display_count - len(all_processed_items))
            if current_display_needed <= 0:
                break
                
            api_url = f"https://openapi.naver.com/v1/search/news.json?query={encoded_query}&display={current_display_needed}&start={start_index}&sort=date"
            
            headers = {
                "X-Naver-Client-Id": client_id,
                "X-Naver-Client-Secret": client_secret,
                "Content-Type": "application/json"
            }
            
            response = requests.get(api_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            if response.status_code == 200:
                news_data = response.json()
                
                if 'items' in news_data and news_data['items']:
                    for item in news_data['items']:
                        try:
                            # 날짜 형식이 다양할 수 있으므로 여러 형식 시도
                            pub_date = None
                            date_formats = [
                                '%a, %d %b %Y %H:%M:%S %z',  # 기본 형식
                                '%Y-%m-%d %H:%M:%S',         # ISO 형식
                                '%Y%m%d'                     # 숫자 형식
                            ]
                            
                            for date_format in date_formats:
                                try:
                                    pub_dt_object = datetime.strptime(item['pubDate'], date_format)
                                    pub_date = pub_dt_object.strftime('%Y%m%d')
                                    break
                                except ValueError:
                                    continue
                            
                            if pub_date:
                                item['pubDate'] = pub_date
                                # HTML 태그 제거 및 텍스트 정리
                                item['title'] = re.sub(r'<[^>]+>', '', item['title']).strip()
                                item['description'] = re.sub(r'<[^>]+>', '', item['description']).strip()
                                all_processed_items.append(item)
                            
                            if len(all_processed_items) >= display_count:
                                return all_processed_items
                                
                        except Exception as e:
                            st.warning(f"기사 처리 중 오류 발생: {str(e)}")
                            continue
                    
                    if not news_data['items']:
                        break
                else:
                    if 'errorMessage' in news_data:
                        st.error(f"API 오류 메시지: {news_data['errorMessage']}")
                    break
                    
            # API 호출 제한에 걸렸을 때 대기
            time.sleep(0.05)  # 50ms로 감소
                
        except requests.exceptions.RequestException as e:
            st.warning(f"API 호출 중 오류 발생: {str(e)}")
            time.sleep(0.5)  # 오류 발생 시 500ms로 감소
            continue
        except json.JSONDecodeError as e:
            st.warning(f"JSON 파싱 오류: {str(e)}")
            continue
        except Exception as e:
            st.warning(f"예상치 못한 오류: {str(e)}")
            continue
            
    return all_processed_items

def extract_featured_stock_names_from_news(news_articles_list, target_date_str, all_stock_names_set):
    """뉴스 기사에서 특징주 정보를 추출합니다. 특징주 관련 첫 번째 기사만 저장합니다."""
    featured_stock_info = {}
    if not all_stock_names_set:
        st.warning("종목명 목록이 비어있어 뉴스에서 종목명을 추출할 수 없습니다.")
        return featured_stock_info
        
    filtered_articles_count = 0
    articles_on_target_date = 0
    
    for article in news_articles_list:
        article_pub_date = article.get("pubDate", "")
        if article_pub_date == target_date_str:
            articles_on_target_date += 1
            title = article.get("title", "")
            description = article.get("description", "")
            
            # HTML 태그 제거 및 텍스트 정리
            title_cleaned = re.sub(r'<[^>]+>', '', title).strip()
            description_cleaned = re.sub(r'<[^>]+>', '', description).strip()
            
            # 특징주 키워드 확장
            if any(keyword in title_cleaned for keyword in ["[특징주]", "특징주", "급등주", "상한가", "강세", "상승"]):
                filtered_articles_count += 1
                
                # 종목명이 제목이나 본문에 있는지 확인
                for stock_name in all_stock_names_set:
                    if stock_name in title_cleaned or stock_name in description_cleaned:
                        # 해당 종목의 첫 번째 특징주 기사만 저장
                        if stock_name not in featured_stock_info:
                            featured_stock_info[stock_name] = [{
                                'title': title_cleaned,
                                'description': description_cleaned,
                                'link': article.get('link', '')
                            }]
    
    if articles_on_target_date == 0:
        st.warning(f"{target_date_str} 날짜의 기사를 찾지 못했습니다.")
    else:
        st.info(f"전체 {articles_on_target_date}개 기사 중 {filtered_articles_count}개의 특징주 관련 기사를 찾았습니다.")
    
    return featured_stock_info

# KRX 기업 정보 로드
krx_company_list_url = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
krx_column_names_map = {
    'source_ticker_col': '종목코드',
    'source_industry_col': '업종',
    'source_products_col': '주요제품'
}
company_details_df_global = load_company_info_from_krx_url(krx_company_list_url, krx_column_names_map)
if company_details_df_global is None:
    st.warning("앱 시작 시 KRX 기업 정보를 로드하지 못했습니다. '업종', '주요제품'은 비어있을 수 있습니다.")
    company_details_df_global = pd.DataFrame(columns=['티커', '업종', '주요제품'])

def get_all_market_data_with_names(date_str, company_info_df):
    """특정 날짜의 전체 시장 데이터를 조회하고 종목명을 포함하여 반환합니다."""
    all_data_frames = []
    company_info_cols_to_add = ['업종', '주요제품']
    base_output_columns = ['티커', '종목명', '시가', '고가', '저가', '종가', '등락률', '거래량', '거래대금', '시장']
    markets_to_fetch = {"KOSPI": "KOSPI", "KOSDAQ": "KOSDAQ", "KONEX": "KONEX"}

    for market_code, market_name in markets_to_fetch.items():
        try:
            # 기본 시장 데이터 조회
            df_market_raw = stock.get_market_ohlcv(date_str, market=market_code)
            if not df_market_raw.empty and '등락률' in df_market_raw.columns:
                df_market = df_market_raw.reset_index()
                if '티커' not in df_market.columns and 'index' in df_market.columns:
                     df_market.rename(columns={'index': '티커'}, inplace=True)

                df_market['시장'] = market_name
                df_market['티커'] = df_market['티커'].astype(str).str.zfill(6)
                
                # 종목명 매핑
                name_map = {ticker: stock.get_market_ticker_name(ticker) for ticker in df_market['티커'] if stock.get_market_ticker_name(ticker)}
                df_market['종목명'] = df_market['티커'].map(name_map)
                
                # 회사 정보 병합
                if company_info_df is not None and not company_info_df.empty:
                    df_market = pd.merge(df_market, company_info_df, on="티커", how="left")
                
                # 업종, 주요제품 컬럼이 없는 경우 빈 문자열로 초기화
                for col in company_info_cols_to_add:
                    if col not in df_market.columns:
                        df_market[col] = ""
                
                current_output_cols = base_output_columns + company_info_cols_to_add
                all_data_frames.append(df_market[current_output_cols])
        except Exception as e:
            st.error(f"{market_name} 전체 데이터 조회 중 오류 발생 ({date_str}): {e}")

    if not all_data_frames:
        st.warning(f"{date_str} 날짜에 조회할 수 있는 전체 시장 데이터가 없습니다.")
        return None
    
    combined_df = pd.concat(all_data_frames, ignore_index=True) 
    for col in ['시가', '고가', '저가', '종가', '등락률', '거래량', '거래대금']:
        if col in combined_df.columns:
            combined_df[col] = pd.to_numeric(combined_df[col], errors='coerce')
    for col in company_info_cols_to_add:
        if col in combined_df.columns:
            combined_df[col] = combined_df[col].fillna("")
        else:
            combined_df[col] = ""
    combined_df = combined_df.dropna(subset=['등락률', '종목명'])
    if combined_df.empty:
        st.warning(f"{date_str} 날짜에 유효한 전체 시장 데이터가 없습니다.")
        return None
    return combined_df

def is_valid_date_format(date_string):
    if not re.match(r"^\d{8}$", date_string): return False
    try:
        datetime.strptime(date_string, '%Y%m%d')
        return True
    except ValueError: return False

def initialize_article_columns(stock_info, start_idx=1, end_idx=5):
    """기사 컬럼을 빈 문자열로 초기화합니다."""
    for i in range(start_idx, end_idx + 1):
        stock_info[f'기사제목{i}'] = ''
        stock_info[f'기사요약{i}'] = ''
        stock_info[f'기사링크{i}'] = ''
    return stock_info

def search_stock_articles_by_date(stock_name, client_id, client_secret, target_date_str, max_count=5, max_retries=3, delay=0.3, match_date=False):
    """종목명으로 네이버 뉴스 검색하여 기사 최대 max_count개 반환"""
    for attempt in range(max_retries):
        try:
            time.sleep(delay)
            encoded_query = quote(stock_name)
            api_url = f"https://openapi.naver.com/v1/search/news.json?query={encoded_query}&display=100&start=1&sort=date"
            headers = {"X-Naver-Client-Id": client_id, "X-Naver-Client-Secret": client_secret}
            response = requests.get(api_url, headers=headers, timeout=10)
            if response.status_code == 429:
                time.sleep(delay * 1.5)  # 429 오류 시 대기 시간 증가율 감소
                delay *= 1.5
                continue
            response.raise_for_status()
            news_data = response.json()
            result = []
            if 'items' in news_data and news_data['items']:
                if match_date:
                    for item in news_data['items']:
                        try:
                            pub_dt_object = datetime.strptime(item['pubDate'], '%a, %d %b %Y %H:%M:%S %z')
                            pub_date_str = pub_dt_object.strftime('%Y%m%d')
                            if pub_date_str == target_date_str:
                                result.append({
                                    'title': re.sub(r'<[^>]+>', '', item['title']).strip(),
                                    'description': re.sub(r'<[^>]+>', '', item['description']).strip(),
                                    'link': item['link']
                                })
                                if len(result) >= max_count:
                                    break
                        except Exception:
                            continue
                else:
                    for item in news_data['items'][:max_count]:
                        result.append({
                            'title': re.sub(r'<[^>]+>', '', item['title']).strip(),
                            'description': re.sub(r'<[^>]+>', '', item['description']).strip(),
                            'link': item['link']
                        })
            return result  # 결과가 없으면 빈 리스트 반환
        except requests.exceptions.RequestException:
            if attempt < max_retries - 1:
                time.sleep(delay * (attempt + 0.5))  # 재시도 시 대기 시간 증가율 감소
            continue
        except Exception:
            return []  # 에러 발생시 빈 리스트 반환
    return []  # 최대 재시도 횟수 초과시 빈 리스트 반환

@st.cache_data
def get_excel_data(df, date_str):
    excel_file = io.BytesIO()
    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='분석결과')
    excel_file.seek(0)
    return excel_file.getvalue()

@st.cache_data
def get_txt_data(df):
    return df.to_csv(sep='\t', index=False)

def format_number(x):
    if pd.isna(x):
        return ""
    try:
        return f"{x:,.0f}"
    except:
        return str(x)

def format_percentage(x):
    if pd.isna(x):
        return ""
    try:
        return f"{x:,.2f}%"
    except:
        return str(x)

def color_negative_red(val):
    """숫자 값에 따라 색상을 반환합니다."""
    try:
        if pd.isna(val):
            return None
        value = float(str(val).replace('%', ''))
        return 'color: red' if value < 0 else 'color: green'
    except:
        return None

def display_analysis_results(final_df_sorted, date_str, all_market_data_df, top_n_count):
    # 결과 표시
    st.success(f"분석이 완료되었습니다. (총 {len(final_df_sorted):,}개 종목)")

    # 날짜 형식 변환 (YYYYMMDD -> YYYY년 MM월 DD일)
    formatted_date = f"{date_str[:4]}년 {date_str[4:6]}월 {date_str[6:8]}일"
    st.markdown(f"""
        <h2 style='text-align: center;'>{formatted_date} 시장 분석</h3>
    """, unsafe_allow_html=True)

    # 전체 종목과 급등주+특징주 분석을 탭으로 구분
    tab1, tab2 = st.tabs(["급등주+특징주 분석", "전체 종목 분석"])
    
    with tab1:
        st.subheader("급등주+특징주 분석")
        
        # 상단 지표 카드들을 한 줄에 배치
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            high_volume_count = len(final_df_sorted[final_df_sorted['거래대금'] >= 10000000000])
            st.metric("거래대금 100억 이상", f"{high_volume_count:,}")
        
        with col2:
            top_featured_count = len(final_df_sorted[final_df_sorted['비고'] == f"top{top_n_count}+특징주"])
            st.metric("Top N + 특징주", f"{top_featured_count:,}")
        
        with col3:
            featured_count = len(final_df_sorted[final_df_sorted['비고'] == "특징주"])
            st.metric("특징주", f"{featured_count:,}")
        
        with col4:
            total_count = len(final_df_sorted)
            st.metric("전체 분석 종목", f"{total_count:,}")
        
        # 상세 결과 테이블
        display_df = final_df_sorted.copy()
        numeric_columns = ['시가', '고가', '저가', '종가', '거래량', '거래대금']
        for col in numeric_columns:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(format_number)
        display_df['등락률'] = display_df['등락률'].apply(format_percentage)
        
        # 거래대금 100억 이상 강조를 위한 스타일 함수
        def highlight_high_amount(val):
            try:
                amount = float(str(val).replace(',', ''))
                return 'color: #FF0000' if amount >= 10000000000 else None
            except:
                return None
        
        styled_df = display_df.style.map(color_negative_red, subset=['등락률']).map(highlight_high_amount, subset=['거래대금'])
        st.dataframe(styled_df, use_container_width=True)

        # 하단에만 다운로드/저장 버튼
        st.subheader("급등주+특징주 데이터 내보내기")
        excel_data = get_excel_data(final_df_sorted, date_str)
        txt_data = get_txt_data(final_df_sorted)
        col1, col2, col3, col4 = st.columns([1,1,1,1])
        with col1:
            st.download_button(
                label="Excel 다운로드",
                data=excel_data,
                file_name=f"stock_analysis_{date_str}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"excel_download_{date_str}"
            )
        with col2:
            st.download_button(
                label="TXT 다운로드",
                data=txt_data,
                file_name=f"stock_analysis_{date_str}.txt",
                mime="text/plain",
                key=f"txt_download_{date_str}"
            )
        with col3:
            if st.button("구글 시트로 내보내기", key=f"google_sheet_{date_str}"):
                with st.spinner("구글 시트로 내보내는 중..."):
                    success, msg = update_google_sheet(final_df_sorted, date_str)
                    if success:
                        st.success(msg)
                    else:
                        st.error(msg)
        with col4:
            if 'db_save_state' not in st.session_state:
                st.session_state.db_save_state = None
            if 'db_overwrite_state' not in st.session_state:
                st.session_state.db_overwrite_state = None
            if st.button("데이터베이스 저장", key=f"db_save_{date_str}"):
                st.session_state.db_save_state = "checking"
                st.session_state.selected_tab = "데이터베이스"
                st.rerun()
            if st.session_state.db_save_state == "checking":
                success, message = save_to_database(final_df_sorted)
                if message == "already_exists":
                    st.warning("이미 저장된 데이터가 있습니다. 덮어쓰시겠습니까?")
                    overwrite_col1, overwrite_col2 = st.columns(2)
                    with overwrite_col1:
                        if st.button("덮어쓰기", key=f"overwrite_{date_str}"):
                            st.session_state.db_overwrite_state = True
                            st.rerun()
                    with overwrite_col2:
                        if st.button("취소", key=f"cancel_{date_str}"):
                            st.session_state.db_save_state = None
                            st.session_state.db_overwrite_state = None
                            st.rerun()
                elif success:
                    st.success(message)
                    st.session_state.db_save_state = None
                else:
                    st.error(message)
                    st.session_state.db_save_state = None
            if st.session_state.db_overwrite_state:
                with st.spinner("데이터를 덮어쓰는 중..."):
                    success, message = save_to_database(final_df_sorted, overwrite=True)
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
                    st.session_state.db_save_state = None
                    st.session_state.db_overwrite_state = None
                    st.rerun()

    with tab2:
        st.subheader(f"전체 종목 (총 {len(all_market_data_df):,}개 종목)")

        # 전체 시장 데이터 표시 (가장 위로)
        market_display_df = all_market_data_df.copy()
        numeric_columns = ['시가', '고가', '저가', '종가', '등락률', '거래량', '거래대금']
        for col in numeric_columns:
            if col in market_display_df.columns:
                market_display_df[col] = market_display_df[col].apply(format_number)
        market_display_df['등락률'] = market_display_df['등락률'].apply(format_percentage)
        styled_market_df = market_display_df.style.map(color_negative_red, subset=['등락률'])
        st.dataframe(styled_market_df, use_container_width=True, height=400)
        st.markdown('<div style="height: 24px;"></div>', unsafe_allow_html=True)

        # 등락률 Top30, 거래대금 Top30 데이터
        top30_rate = all_market_data_df.nlargest(30, '등락률')
        top30_amount = all_market_data_df.nlargest(30, '거래대금')

        # 4개 컬럼으로 한 줄에 배치
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown("<div style='text-align:center; font-weight:bold; font-size:1.1em;'>등락률 Top30</div>", unsafe_allow_html=True)
            top30_rate_table = top30_rate[['종목명', '등락률', '업종', '주요제품']].copy()
            top30_rate_table['등락률'] = top30_rate_table['등락률'].apply(format_percentage)
            st.dataframe(
                top30_rate_table.style.set_table_styles([
                    {'selector': 'td', 'props': [('font-size', '0.95em')]},
                    {'selector': 'th', 'props': [('font-size', '0.95em')]}
                ]),
                use_container_width=True, hide_index=True
            )

        with col2:
            st.markdown("<div style='text-align:center; font-weight:bold; font-size:1.1em;'>등락률 Top30 시장별 분포</div>", unsafe_allow_html=True)
            fig1 = px.pie(top30_rate, names='시장', title=None)
            st.plotly_chart(fig1, use_container_width=True)

        with col3:
            st.markdown("<div style='text-align:center; font-weight:bold; font-size:1.1em;'>거래대금 Top30</div>", unsafe_allow_html=True)
            top30_amount_table = top30_amount[['종목명', '거래대금', '업종', '주요제품']].copy()
            top30_amount_table['거래대금'] = top30_amount_table['거래대금'].apply(format_number)
            st.dataframe(
                top30_amount_table.style.set_table_styles([
                    {'selector': 'td', 'props': [('font-size', '0.95em')]},
                    {'selector': 'th', 'props': [('font-size', '0.95em')]}
                ]),
                use_container_width=True, hide_index=True
            )

        with col4:
            st.markdown("<div style='text-align:center; font-weight:bold; font-size:1.1em;'>거래대금 Top30 시장별 분포</div>", unsafe_allow_html=True)
            fig2 = px.pie(top30_amount, names='시장', title=None)
            st.plotly_chart(fig2, use_container_width=True)

        # 전체 시장 거래대금 표시
        try:
            df_total_investor = stock.get_market_trading_value_by_date(date_str, date_str, "ALL", etf=True, etn=True, elw=True)
            if not df_total_investor.empty:
                # '전체' 컬럼 제거
                if '전체' in df_total_investor.columns:
                    df_total_investor = df_total_investor.drop(columns=['전체'])
                # '날짜' 컬럼 제거
                if '날짜' in df_total_investor.columns:
                    df_total_investor = df_total_investor.drop(columns=['날짜'])
                # 가공된 테이블만 출력
                styled_df = df_total_investor.copy()
                for col in styled_df.columns:
                    if pd.api.types.is_numeric_dtype(styled_df[col]):
                        styled_df[col] = styled_df[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "")
                def style_color(val):
                    try:
                        v = float(str(val).replace(',', ''))
                        if v > 0:
                            return 'color: #FF0000'
                        elif v < 0:
                            return 'color: #0000FF'
                    except:
                        return None
                st.markdown("#### 투자자별 거래대금(KOSPI+KOSDAQ+KONEX)")
                st.dataframe(styled_df.style.map(style_color), use_container_width=True)
        except Exception as e:
            st.warning(f"전체 시장 거래대금 정보 조회 중 오류 발생: {e}")

        # 시장별 투자자 정보 표시 (KOSPI/KOSDAQ 통합)
        try:
            df_kospi = stock.get_market_trading_value_by_date(date_str, date_str, "KOSPI", etf=True, etn=True, elw=True, detail=True)
            df_kosdaq = stock.get_market_trading_value_by_date(date_str, date_str, "KOSDAQ", etf=True, etn=True, elw=True, detail=True)
            if not df_kospi.empty:
                df_kospi["시장"] = "KOSPI"
            if not df_kosdaq.empty:
                df_kosdaq["시장"] = "KOSDAQ"
            # 공통 컬럼만 사용
            if not df_kospi.empty and not df_kosdaq.empty:
                common_cols = list(set(df_kospi.columns) & set(df_kosdaq.columns))
                # 시장 컬럼이 맨 앞으로 오도록
                if "시장" in common_cols:
                    common_cols = ["시장"] + [col for col in df_kospi.columns if col != "시장" and col in common_cols]
                df_merged = pd.concat([df_kospi[common_cols], df_kosdaq[common_cols]], ignore_index=True)
            elif not df_kospi.empty:
                df_merged = df_kospi.copy()
            elif not df_kosdaq.empty:
                df_merged = df_kosdaq.copy()
            else:
                df_merged = pd.DataFrame()
            if not df_merged.empty:
                # '전체' 컬럼 제거
                if '전체' in df_merged.columns:
                    df_merged = df_merged.drop(columns=['전체'])
                # 숫자 포맷 및 색상 스타일
                for col in df_merged.columns:
                    if pd.api.types.is_numeric_dtype(df_merged[col]):
                        df_merged[col] = df_merged[col].apply(lambda x: f"{x:,.0f}" if pd.notnull(x) else "")
                def style_color(val):
                    try:
                        v = float(str(val).replace(',', ''))
                        if v > 0:
                            return 'color: #FF0000'
                        elif v < 0:
                            return 'color: #0000FF'
                    except:
                        return None
                st.markdown("#### KOSPI/KOSDAQ 투자자별 거래대금")
                st.dataframe(df_merged.style.map(style_color), use_container_width=True, hide_index=True)
        except Exception as e:
            st.warning(f"KOSPI/KOSDAQ 투자자 정보 조회 중 오류 발생: {e}")

def init_database():
    """SQLite 데이터베이스 초기화"""
    try:
        conn = sqlite3.connect('stock_analysis.db')
        c = conn.cursor()
        
        # 테이블이 없을 때만 생성 (테마, AI_한줄요약 컬럼 포함)
        c.execute('''
            CREATE TABLE IF NOT EXISTS stock_analysis (
                날짜 TEXT,
                티커 TEXT,
                종목명 TEXT,
                업종 TEXT,
                주요제품 TEXT,
                시가 REAL,
                고가 REAL,
                저가 REAL,
                종가 REAL,
                등락률 REAL,
                거래량 INTEGER,
                거래대금 INTEGER,
                시장 TEXT,
                비고 TEXT,
                기사제목1 TEXT,
                기사요약1 TEXT,
                기사링크1 TEXT,
                기사제목2 TEXT,
                기사요약2 TEXT,
                기사링크2 TEXT,
                기사제목3 TEXT,
                기사요약3 TEXT,
                기사링크3 TEXT,
                기사제목4 TEXT,
                기사요약4 TEXT,
                기사링크4 TEXT,
                기사제목5 TEXT,
                기사요약5 TEXT,
                기사링크5 TEXT,
                PRIMARY KEY (날짜, 티커)
            )
        ''')
        
        # 기존 테이블에 새 컬럼 추가 (이미 있으면 무시)
        for col in ["테마", "AI_한줄요약"]:
            try:
                c.execute(f"ALTER TABLE stock_analysis ADD COLUMN {col} TEXT")
            except sqlite3.OperationalError:
                pass
        
        # 기존 테이블에 새 기사 컬럼 추가
        try:
            c.execute("ALTER TABLE stock_analysis ADD COLUMN 기사제목4 TEXT")
            c.execute("ALTER TABLE stock_analysis ADD COLUMN 기사요약4 TEXT")
            c.execute("ALTER TABLE stock_analysis ADD COLUMN 기사링크4 TEXT")
            c.execute("ALTER TABLE stock_analysis ADD COLUMN 기사제목5 TEXT")
            c.execute("ALTER TABLE stock_analysis ADD COLUMN 기사요약5 TEXT")
            c.execute("ALTER TABLE stock_analysis ADD COLUMN 기사링크5 TEXT")
        except sqlite3.OperationalError as e:
            # 컬럼이 이미 존재하는 경우 무시
            pass
        
        conn.commit()
        conn.close()
    except Exception as e:
        st.error(f"데이터베이스 초기화 중 오류 발생: {str(e)}")
        if conn:
            conn.close()

def reset_database():
    """데이터베이스를 완전히 초기화 (모든 데이터 삭제)"""
    try:
        conn = sqlite3.connect('stock_analysis.db')
        c = conn.cursor()
        
        # 기존 테이블 삭제
        c.execute("DROP TABLE IF EXISTS stock_analysis")
        
        # 테이블 다시 생성
        c.execute('''
            CREATE TABLE IF NOT EXISTS stock_analysis (
                날짜 TEXT,
                티커 TEXT,
                종목명 TEXT,
                업종 TEXT,
                주요제품 TEXT,
                시가 REAL,
                고가 REAL,
                저가 REAL,
                종가 REAL,
                등락률 REAL,
                거래량 INTEGER,
                거래대금 INTEGER,
                시장 TEXT,
                비고 TEXT,
                기사제목1 TEXT,
                기사요약1 TEXT,
                기사링크1 TEXT,
                기사제목2 TEXT,
                기사요약2 TEXT,
                기사링크2 TEXT,
                기사제목3 TEXT,
                기사요약3 TEXT,
                기사링크3 TEXT,
                기사제목4 TEXT,
                기사요약4 TEXT,
                기사링크4 TEXT,
                기사제목5 TEXT,
                기사요약5 TEXT,
                기사링크5 TEXT,
                PRIMARY KEY (날짜, 티커)
            )
        ''')
        
        conn.commit()
        conn.close()
        st.success("데이터베이스가 초기화되었습니다.")
    except Exception as e:
        st.error(f"데이터베이스 초기화 중 오류 발생: {str(e)}")
        if conn:
            conn.close()

def save_to_database(df, overwrite=False):
    """데이터프레임을 SQLite 데이터베이스에 저장"""
    if df is None or df.empty:
        return False, "저장할 데이터가 없습니다."

    conn = None
    try:
        conn = sqlite3.connect('stock_analysis.db')
        cursor = conn.cursor()
        date_str = df['날짜'].iloc[0]

        # 데이터 존재 여부 확인
        cursor.execute("SELECT COUNT(*) FROM stock_analysis WHERE 날짜 = ?", (date_str,))
        exists = cursor.fetchone()[0] > 0

        if exists and not overwrite:
            return False, "already_exists"

        # 덮어쓰기 모드이거나 데이터가 없는 경우
        if exists and overwrite:
            # 기존 데이터 삭제
            cursor.execute("DELETE FROM stock_analysis WHERE 날짜 = ?", (date_str,))
            conn.commit()
            st.info(f"{date_str} 날짜의 기존 데이터를 삭제했습니다.")

        # 데이터 전처리
        df_to_save = df.copy()
        
        # 숫자형 컬럼 변환
        numeric_columns = {
            'float': ['시가', '고가', '저가', '종가', '등락률'],
            'int': ['거래량', '거래대금']
        }
        
        for col in numeric_columns['float']:
            df_to_save[col] = pd.to_numeric(df_to_save[col], errors='coerce')
        
        for col in numeric_columns['int']:
            df_to_save[col] = pd.to_numeric(df_to_save[col], errors='coerce').fillna(0).astype('int64')
        
        # 문자열 컬럼 전처리
        string_columns = [col for col in df_to_save.columns if col not in 
                         numeric_columns['float'] + numeric_columns['int']]
        
        for col in string_columns:
            df_to_save[col] = df_to_save[col].fillna('').astype(str)

        # 데이터 저장
        placeholders = ','.join(['?' for _ in df_to_save.columns])
        insert_sql = f'''
            INSERT INTO stock_analysis (
                {','.join(df_to_save.columns)}
            ) VALUES ({placeholders})
        '''
        
        # 데이터를 리스트로 변환하여 한 번에 저장
        data_to_insert = df_to_save.values.tolist()
        
        # 청크 단위로 데이터 저장
        chunk_size = 500
        for i in range(0, len(data_to_insert), chunk_size):
            chunk = data_to_insert[i:i + chunk_size]
            cursor.executemany(insert_sql, chunk)
            conn.commit()
        
        # 저장된 데이터 수 확인
        cursor.execute("SELECT COUNT(*) FROM stock_analysis WHERE 날짜 = ?", (date_str,))
        saved_count = cursor.fetchone()[0]
        
        # 원본 데이터와 저장된 데이터 수 비교
        if saved_count != len(df):
            return False, f"데이터 저장 불일치: 원본 {len(df)}개, 저장됨 {saved_count}개"
        
        conn.close()
        return True, f"데이터베이스 저장 완료 (저장된 데이터: {saved_count}개)"
        
    except Exception as e:
        if conn:
            conn.close()
        return False, f"데이터베이스 저장 중 오류 발생: {str(e)}"

def get_saved_dates():
    """저장된 날짜 목록 조회"""
    conn = sqlite3.connect('stock_analysis.db')
    c = conn.cursor()
    c.execute("SELECT DISTINCT 날짜 FROM stock_analysis ORDER BY 날짜 DESC")
    dates = [row[0] for row in c.fetchall()]
    conn.close()
    return dates

def get_analysis_by_date(date_str):
    """특정 날짜의 분석 결과 조회"""
    try:
        conn = sqlite3.connect('stock_analysis.db')
        query = "SELECT * FROM stock_analysis WHERE 날짜 = ?"
        df = pd.read_sql_query(query, conn, params=(date_str,))
        conn.close()
        return df
    except Exception as e:
        st.error(f"데이터 조회 중 오류 발생: {str(e)}")
        if conn:
            conn.close()
        return pd.DataFrame()

def get_data_by_date_range(start_date, end_date):
    """특정 기간의 분석 결과를 조회"""
    try:
        conn = sqlite3.connect('stock_analysis.db')
        query = "SELECT * FROM stock_analysis WHERE 날짜 BETWEEN ? AND ?"
        df = pd.read_sql_query(query, conn, params=(start_date, end_date))
        conn.close()
        return df
    except Exception as e:
        st.error(f"데이터 조회 중 오류 발생: {str(e)}")
        if conn:
            conn.close()
        return pd.DataFrame()

def create_market_distribution_pie(df):
    """시장별 종목 분포 파이 차트 생성"""
    market_counts = df.groupby('시장').size().reset_index(name='count')
    fig = px.pie(
        market_counts, 
        values='count', 
        names='시장',
        title='시장별 종목 분포',
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    fig.update_traces(textposition='inside', textinfo='percent+label')
    return fig

def create_top_rate_changes_bar(df):
    """등락률 상위 10개 종목 막대 그래프 생성"""
    # 각 종목의 최신 데이터만 사용
    latest_data = df.sort_values('날짜').groupby('종목명').last()
    top_changes = latest_data.nlargest(10, '등락률')[['등락률', '시장']]
    
    fig = go.Figure()
    for market in top_changes['시장'].unique():
        market_data = top_changes[top_changes['시장'] == market]
        fig.add_trace(go.Bar(
            x=market_data.index,
            y=market_data['등락률'],
            name=market,
            text=market_data['등락률'].round(2).astype(str) + '%',
            textposition='auto',
        ))
    
    fig.update_layout(
        title='등락률 상위 10개 종목',
        xaxis_title='종목명',
        yaxis_title='등락률 (%)',
        barmode='group',
        showlegend=True
    )
    return fig

def create_top_volume_bar(df):
    """거래량 상위 10개 종목 막대 그래프 생성"""
    # 각 종목의 최신 데이터만 사용
    latest_data = df.sort_values('날짜').groupby('종목명').last()
    top_volume = latest_data.nlargest(10, '거래량')[['거래량', '시장']]
    
    fig = go.Figure()
    for market in top_volume['시장'].unique():
        market_data = top_volume[top_volume['시장'] == market]
        fig.add_trace(go.Bar(
            x=market_data.index,
            y=market_data['거래량'],
            name=market,
            text=(market_data['거래량'] / 1000000).round(2).astype(str) + 'M',
            textposition='auto',
        ))
    
    fig.update_layout(
        title='거래량 상위 10개 종목',
        xaxis_title='종목명',
        yaxis_title='거래량',
        barmode='group',
        showlegend=True
    )
    return fig

def create_industry_distribution_bar(df):
    """업종별 종목 수 분포 막대 그래프 생성"""
    # 각 종목의 최신 데이터만 사용하여 중복 제거
    latest_data = df.sort_values('날짜').groupby('종목명').last()
    industry_counts = latest_data.groupby('업종').size().sort_values(ascending=False)
    
    fig = go.Figure(go.Bar(
        x=industry_counts.index,
        y=industry_counts.values,
        text=industry_counts.values,
        textposition='auto',
    ))
    
    fig.update_layout(
        title='업종별 종목 수 분포',
        xaxis_title='업종',
        yaxis_title='종목 수',
        xaxis_tickangle=-45,
        height=600  # 업종명이 잘 보이도록 높이 조정
    )
    return fig

# --- Streamlit UI ---

# 앱 시작시 데이터베이스 초기화
init_database()

# 세션 상태 초기화
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = None
if 'analysis_date' not in st.session_state:
    st.session_state.analysis_date = None
if 'all_market_data' not in st.session_state:
    st.session_state.all_market_data = None

def read_google_sheet(worksheet_name=None):
    sheet = get_google_sheet()
    if not sheet:
        st.error("구글 시트 연결 실패")
        return None, []
    worksheet_list = sheet.worksheets()
    worksheet_names = [ws.title for ws in worksheet_list]
    if not worksheet_names:
        st.warning("구글 시트에 워크시트가 없습니다.")
        return None, worksheet_names
    # 워크시트 선택
    if worksheet_name is None:
        worksheet_name = worksheet_names[-1]  # 기본값: 마지막 워크시트
    worksheet = sheet.worksheet(worksheet_name)
    data = worksheet.get_all_records()
    if not data:
        st.warning(f"{worksheet_name} 워크시트에 데이터가 없습니다.")
        return None, worksheet_names
    import pandas as pd
    df = pd.DataFrame(data)
    return df, worksheet_names

# 4개의 탭 생성 (복원)
tab1, tab2, tab3, tab4 = st.tabs(["실시간 분석", "데이터베이스", "인포그래픽", "구글 시트 보기"])

# 실시간 분석 탭
with tab1:
    # 분석 설정
    col1, col2, col3 = st.columns(3)
    with col1:
        input_date = st.date_input(
            "조회 날짜",
            value=datetime.now().date(),
            format="YYYY-MM-DD"
        )
    with col2:
        top_n_count = st.number_input(
            "상위 종목수",
            min_value=1,
            max_value=100,
            value=40,
            step=1
        )
    with col3:
        news_display_count = st.number_input(
            "특징주 기사 검색수",
            min_value=1,
            max_value=1000,
            value=500,
            step=1
        )

    # 분석 실행 버튼과 다운로드 버튼을 나란히 배치
    col1, _, _ = st.columns([2,1,1])
    with col1:
        run_analysis = st.button("분석 실행", type="primary")

    # 분석 실행
    if run_analysis:
        try:
            # 입력값 검증
            date_str = input_date.strftime("%Y%m%d")
            if not is_valid_date_format(date_str):
                st.error("잘못된 날짜 형식입니다.")
                st.stop()

            progress_text = "분석이 진행 중입니다..."
            progress_bar = st.progress(0, text=progress_text)

            # 전체 시장 데이터 조회
            progress_bar.progress(0.5, text="시장 데이터 조회 준비 중...")
            all_market_data_df = get_all_market_data_with_names(date_str, company_details_df_global)
            if all_market_data_df is None or all_market_data_df.empty:
                st.error(f"{date_str} 날짜의 시장 데이터를 찾을 수 없습니다.")
                st.stop()
            progress_bar.progress(1.0, text="시장 데이터 조회 완료")

            # 등락률 기준 상위 N개 종목 선택
            progress_bar.progress(0.20, text="상위 종목 선별 중...")
            top_n_df = all_market_data_df.sort_values(by='등락률', ascending=False).head(top_n_count)
            top_n_stock_names = set(top_n_df['종목명'].tolist())
            progress_bar.progress(0.30, text="상위 종목 선별 완료")

            # 특징주 뉴스 검색
            progress_bar.progress(0.35, text="특징주 뉴스 검색 준비 중...")
            featured_stock_info = {}
            if NAVER_CLIENT_ID and NAVER_CLIENT_SECRET:
                progress_bar.progress(0.40, text="네이버 뉴스 API 호출 중...")
                news_articles = call_naver_search_api("특징주", news_display_count, NAVER_CLIENT_ID, NAVER_CLIENT_SECRET)
                progress_bar.progress(0.50, text="특징주 정보 추출 중...")
                featured_stock_info = extract_featured_stock_names_from_news(news_articles, date_str, set(all_market_data_df['종목명']))
            progress_bar.progress(0.60, text="특징주 뉴스 검색 완료")

            # 최종 데이터프레임 생성
            progress_bar.progress(0.85, text="데이터프레임 생성 중...")
            final_data_list = []
            processed_stocks = set()

            # Top N 종목 처리
            progress_bar.progress(0.35, text="Top N 종목 데이터 수집 중...")
            for idx, (_, row) in enumerate(top_n_df.iterrows(), 1):
                stock_name = row['종목명']
                if stock_name in processed_stocks:
                    continue

                progress_bar.progress(0.35 + (idx/len(top_n_df))*0.20,
                    text=f"Top N 종목 처리 중... ({idx}/{len(top_n_df)}) - {stock_name}")

                processed_stocks.add(stock_name)
                stock_info = {
                    '날짜': date_str,
                    '티커': row['티커'],
                    '종목명': stock_name,
                    '업종': row.get('업종', ''),
                    '주요제품': row.get('주요제품', ''),
                    '시가': row['시가'],
                    '고가': row['고가'],
                    '저가': row['저가'],
                    '종가': row['종가'],
                    '등락률': row['등락률'],
                    '거래량': row['거래량'],
                    '거래대금': row['거래대금'],
                    '시장': row['시장'],
                    '비고': ''
                }

                # 기사 컬럼 초기화
                initialize_article_columns(stock_info)

                if stock_name in featured_stock_info:
                    stock_info['비고'] = f"top{top_n_count}+특징주"
                    # 첫 번째 기사는 특징주 기사로 설정
                    first_article = featured_stock_info[stock_name][0]
                    stock_info['기사제목1'] = first_article['title']
                    stock_info['기사요약1'] = first_article['description']
                    stock_info['기사링크1'] = first_article['link']

                    # 추가 기사 4개 검색
                    if NAVER_CLIENT_ID and NAVER_CLIENT_SECRET:
                        progress_bar.progress(0.35 + (idx/len(top_n_df))*0.20,
                            text=f"Top N 종목 추가 기사 검색 중... ({idx}/{len(top_n_df)}) - {stock_name}")
                        additional_articles = search_stock_articles_by_date(
                            stock_name,
                            NAVER_CLIENT_ID,
                            NAVER_CLIENT_SECRET,
                            date_str,
                            max_count=4,
                            match_date=True
                        )
                        # 기사2~5에 매핑
                        for i, article in enumerate(additional_articles, 2):
                            stock_info[f'기사제목{i}'] = article['title']
                            stock_info[f'기사요약{i}'] = article['description']
                            stock_info[f'기사링크{i}'] = article['link']
                else:
                    stock_info['비고'] = f"top{top_n_count}"
                    # 일반 종목은 기사 5개 검색
                    if NAVER_CLIENT_ID and NAVER_CLIENT_SECRET:
                        progress_bar.progress(0.35 + (idx/len(top_n_df))*0.20,
                            text=f"Top N 종목 기사 검색 중... ({idx}/{len(top_n_df)}) - {stock_name}")
                        articles = search_stock_articles_by_date(
                            stock_name,
                            NAVER_CLIENT_ID,
                            NAVER_CLIENT_SECRET,
                            date_str,
                            max_count=5,
                            match_date=True
                        )
                        for i, article in enumerate(articles, 1):
                            stock_info[f'기사제목{i}'] = article['title']
                            stock_info[f'기사요약{i}'] = article['description']
                            stock_info[f'기사링크{i}'] = article['link']

                final_data_list.append(stock_info)
            progress_bar.progress(0.55, text="Top N 종목 처리 완료")

            # 특징주 정보 추가
            progress_bar.progress(0.60, text="특징주 정보 수집 시작...")
            featured_stocks = [stock for stock in featured_stock_info.keys() if stock not in processed_stocks]
            for idx, stock_name in enumerate(featured_stocks, 1):
                progress_bar.progress(0.60 + (idx/len(featured_stocks))*0.20,
                    text=f"특징주 정보 처리 중... ({idx}/{len(featured_stocks)}) - {stock_name}")
                try:
                    stock_row = all_market_data_df[all_market_data_df['종목명'] == stock_name].iloc[0]
                    stock_info = {
                        '날짜': date_str,
                        '티커': stock_row['티커'],
                        '종목명': stock_name,
                        '업종': stock_row.get('업종', ''),
                        '주요제품': stock_row.get('주요제품', ''),
                        '시가': stock_row['시가'],
                        '고가': stock_row['고가'],
                        '저가': stock_row['저가'],
                        '종가': stock_row['종가'],
                        '등락률': stock_row['등락률'],
                        '거래량': stock_row['거래량'],
                        '거래대금': stock_row['거래대금'],
                        '시장': stock_row['시장'],
                        '비고': '특징주'
                    }

                    # 기사 컬럼 초기화
                    initialize_article_columns(stock_info)

                    # 첫 번째 기사는 특징주 기사
                    first_article = featured_stock_info[stock_name][0]
                    stock_info['기사제목1'] = first_article['title']
                    stock_info['기사요약1'] = first_article['description']
                    stock_info['기사링크1'] = first_article['link']

                    # 추가 기사 4개 검색
                    if NAVER_CLIENT_ID and NAVER_CLIENT_SECRET:
                        progress_bar.progress(0.60 + (idx/len(featured_stocks))*0.20,
                            text=f"특징주 추가 기사 검색 중... ({idx}/{len(featured_stocks)}) - {stock_name}")
                        additional_articles = search_stock_articles_by_date(
                            stock_name,
                            NAVER_CLIENT_ID,
                            NAVER_CLIENT_SECRET,
                            date_str,
                            max_count=4,
                            match_date=True
                        )

                        # 기사2~5에 매핑
                        for i, article in enumerate(additional_articles, 2):
                            stock_info[f'기사제목{i}'] = article['title']
                            stock_info[f'기사요약{i}'] = article['description']
                            stock_info[f'기사링크{i}'] = article['link']

                    final_data_list.append(stock_info)
                    processed_stocks.add(stock_name)
                except Exception as e:
                    st.error(f"특징주 {stock_name} 처리 중 오류 발생: {str(e)}")
                    continue
            progress_bar.progress(0.80, text="특징주 정보 처리 완료")

            # 최종 데이터프레임 생성 및 정렬
            progress_bar.progress(0.85, text="데이터프레임 생성 중...")
            final_df = pd.DataFrame(final_data_list)
            progress_bar.progress(0.90, text="데이터 정렬 중...")
            final_df_sorted = final_df.sort_values(by='등락률', ascending=False)

            progress_bar.progress(0.95, text="분석 결과 저장 중...")

            # 분석 결과를 세션 상태에 저장
            st.session_state.analysis_results = final_df_sorted
            st.session_state.analysis_date = date_str
            st.session_state.all_market_data = all_market_data_df

            progress_bar.progress(1.0, text="분석이 완료되었습니다!")
            time.sleep(1)
            progress_bar.empty()

            # 결과 표시
            display_analysis_results(final_df_sorted, date_str, all_market_data_df, top_n_count)

        except Exception as e:
            st.error(f"분석 중 오류가 발생했습니다: {str(e)}")
            st.error("상세 오류:")
            st.exception(e)

    # 이전 분석 결과 표시 (세션에 저장된 결과가 있을 경우)
    elif st.session_state.analysis_results is not None:
        display_analysis_results(
            st.session_state.analysis_results,
            st.session_state.analysis_date,
            st.session_state.all_market_data,
            top_n_count
        )

# 데이터베이스 탭
with tab2:
    saved_dates = get_saved_dates()
    st.subheader("급등주+특징주 분석 결과 기간별 조회")
    # 저장된 날짜 목록 가져오기
    if saved_dates:
        # 기간 선택 UI
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "시작 날짜",
                value=datetime.strptime(min(saved_dates), '%Y%m%d').date(),
                min_value=datetime.strptime(min(saved_dates), '%Y%m%d').date(),
                max_value=datetime.strptime(max(saved_dates), '%Y%m%d').date(),
                format="YYYY-MM-DD"
            )
        with col2:
            end_date = st.date_input(
                "종료 날짜",
                value=datetime.strptime(max(saved_dates), '%Y%m%d').date(),
                min_value=datetime.strptime(min(saved_dates), '%Y%m%d').date(),
                max_value=datetime.strptime(max(saved_dates), '%Y%m%d').date(),
                format="YYYY-MM-DD"
            )

        if start_date <= end_date:
            start_date_str = start_date.strftime('%Y%m%d')
            end_date_str = end_date.strftime('%Y%m%d')
            period_data = get_data_by_date_range(start_date_str, end_date_str)

            if not period_data.empty:
                # 컬럼 순서 재정렬: 종목명 뒤에 테마, AI_한줄요약
                db_columns = [
                    '날짜', '티커', '종목명',
                    '업종', '주요제품', '시가', '고가', '저가', '종가', '등락률', '거래량', '거래대금', '시장', '비고',
                    '기사제목1', '기사요약1', '기사링크1',
                    '기사제목2', '기사요약2', '기사링크2',
                    '기사제목3', '기사요약3', '기사링크3',
                    '기사제목4', '기사요약4', '기사링크4',
                    '기사제목5', '기사요약5', '기사링크5'
                ]
                for col in db_columns:
                    if col not in period_data.columns:
                        period_data[col] = ""
                period_data = period_data[db_columns]

                # 상세 결과 테이블
                display_df = period_data.copy()
                numeric_columns = ['시가', '고가', '저가', '종가', '거래량', '거래대금']
                for col in numeric_columns:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].apply(format_number)
                display_df['등락률'] = display_df['등락률'].apply(format_percentage)

                styled_df = display_df.style.map(color_negative_red, subset=['등락률'])
                st.dataframe(styled_df, use_container_width=True)

                # 데이터 내보내기
                col1, col2 = st.columns(2)
                with col1:
                    excel_data = get_excel_data(period_data, f"{start_date_str}-{end_date_str}")
                    st.download_button(
                        label="Excel 다운로드",
                        data=excel_data,
                        file_name=f"stock_analysis_{start_date_str}-{end_date_str}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                with col2:
                    txt_data = get_txt_data(period_data)
                    st.download_button(
                        label="TXT 다운로드",
                        data=txt_data,
                        file_name=f"stock_analysis_{start_date_str}-{end_date_str}.txt",
                        mime="text/plain"
                    )
            else:
                st.warning("선택한 기간에 저장된 데이터가 없습니다.")
        else:
            st.error("종료 날짜는 시작 날짜보다 커야 합니다.")
    else:
        st.info("저장된 분석 결과가 없습니다.")

# 인포그래픽 탭
with tab3:
    saved_dates = get_saved_dates()
    st.subheader("급등주+특징주 기간별 분석 인포그래픽")
    # 저장된 전체 날짜 범위 확인
    if saved_dates:
        saved_dates_dt = [datetime.strptime(date, '%Y%m%d').date() for date in saved_dates]
        min_date = min(saved_dates_dt)
        max_date = max(saved_dates_dt)

        # 기간 선택 UI
        col1, col2 = st.columns(2)
        with col1:
            # 시작 날짜는 max_date보다 하루 전으로 설정
            default_start_date = max_date if max_date == min_date else max_date - timedelta(days=1)
            viz_start_date = st.date_input(
                "시작 날짜",
                value=default_start_date,
                min_value=min_date,
                max_value=max_date,
                format="YYYY-MM-DD",
                key="viz_start_date"
            )
        with col2:
            viz_end_date = st.date_input(
                "종료 날짜",
                value=max_date,
                min_value=min_date,
                max_value=max_date,
                format="YYYY-MM-DD",
                key="viz_end_date"
            )

        if viz_start_date <= viz_end_date:
            # 선택된 기간의 데이터 조회
            viz_start_date_str = viz_start_date.strftime('%Y%m%d')
            viz_end_date_str = viz_end_date.strftime('%Y%m%d')
            period_data = get_data_by_date_range(viz_start_date_str, viz_end_date_str)

            if not period_data.empty:
                # 4개의 차트를 2x2 그리드로 배치
                col1, col2 = st.columns(2)
                with col1:
                    st.plotly_chart(create_market_distribution_pie(period_data), use_container_width=True)
                    st.plotly_chart(create_top_volume_bar(period_data), use_container_width=True)
                with col2:
                    st.plotly_chart(create_top_rate_changes_bar(period_data), use_container_width=True)
                    st.plotly_chart(create_industry_distribution_bar(period_data), use_container_width=True)
            else:
                st.warning("선택한 기간에 저장된 데이터가 없습니다.")
        else:
            st.error("종료 날짜는 시작 날짜보다 커야 합니다.")
    else:
        st.info("저장된 분석 결과가 없습니다.")

# 구글 시트 보기 탭
with tab4:
    st.subheader("구글 시트 데이터 보기")
    # 워크시트 목록 불러오기 및 선택
    sheet = get_google_sheet()
    if sheet:
        worksheet_list = sheet.worksheets()
        worksheet_names = [ws.title for ws in worksheet_list]
        if worksheet_names:
            selected_ws = st.selectbox("워크시트 선택", worksheet_names, index=len(worksheet_names)-1)
            df, _ = read_google_sheet(selected_ws)
            if df is not None and not df.empty:
                st.dataframe(df, use_container_width=True)
                st.success(f"{selected_ws} 워크시트의 데이터를 불러왔습니다.")
            else:
                st.warning(f"{selected_ws} 워크시트에 데이터가 없습니다.")
        else:
            st.warning("구글 시트에 워크시트가 없습니다.")
    else:
        st.error("구글 시트 연결 실패")

# 도움말
with st.expander("도움말"):
    st.markdown("""
    ## 📖 사용 가이드

    이 앱은 실시간 시장 데이터와 뉴스 분석을 통해 급등주와 특징주를 탐지하고, 다양한 시각화와 데이터 내보내기 기능을 제공합니다.

    ---
    ### 1️⃣ 실시간 분석 탭
    - **조회 날짜**: 분석할 날짜를 선택합니다. (영업일 기준, 장중에는 기사 수가 적을 수 있습니다)
    - **상위 종목수**: 등락률 기준으로 상위 몇 개 종목을 분석할지 입력합니다. (예: 40)
    - **특징주 기사 검색수**: 네이버 뉴스에서 '특징주' 키워드로 검색할 기사 수를 입력합니다. (예: 500)
    - **분석 실행**: 버튼을 클릭하면 실시간 시장 데이터와 뉴스 기사 분석이 시작됩니다.
    - **분석 결과**: 
        - '급등주+특징주 분석' 탭에서 Top N 종목과 특징주, 관련 뉴스 기사, 데이터 내보내기(Excel, TXT, DB 저장) 기능을 제공합니다.
        - '전체 종목 분석' 탭에서 전체 시장 데이터, Top30 등락률/거래대금, 투자자별 거래대금, 시장별 투자자 정보 등을 확인할 수 있습니다.

    ---
    ### 2️⃣ 데이터베이스 탭
    - **기간별 조회**: 저장된 분석 결과를 시작/종료 날짜로 조회할 수 있습니다.
    - **결과 테이블**: 해당 기간의 모든 분석 결과를 표로 확인할 수 있습니다.
    - **데이터 내보내기**: Excel, TXT 파일로 다운로드할 수 있습니다.

    ---
    ### 3️⃣ 인포그래픽 탭
    - **기간 선택**: 저장된 데이터 중 원하는 기간을 선택합니다.
    - **시각화**: 시장별 종목 분포, 등락률 상위 10개, 거래량 상위 10개, 업종별 종목 수 분포 등 다양한 차트를 제공합니다.

    ---
    ### 주요 기능 설명
    - **Top N + 특징주**: 등락률 상위 N개 종목과 뉴스에서 특징주로 언급된 종목
    - **특징주**: 네이버 기사에서 특징주로 언급된 종목
    - **DB 저장**: 분석 결과를 데이터베이스에 저장하여 나중에 조회/시각화할 수 있습니다.
    - **Excel/TXT 다운로드**: 분석 결과를 파일로 저장할 수 있습니다.

    ---
    ### 결과 해석 팁
    - **거래대금 100억 이상**: 대형 거래가 발생한 종목을 빠르게 파악할 수 있습니다.
    - **Top N + 특징주/특징주**: 뉴스와 시장 데이터가 동시에 주목하는 종목을 확인하세요.
    - **시장별/업종별 분포**: 특정 시장이나 업종에 급등주가 몰려 있는지 한눈에 볼 수 있습니다.

    ---
    ### 자주 묻는 질문 (FAQ)
    - **Q. 분석 결과가 비어있어요!**
        - 영업일이 아니거나, 장 마감 전에는 데이터/뉴스가 부족할 수 있습니다.
        - 네트워크 연결 또는 API 키 설정을 확인하세요.
    - **Q. DB 저장이 안 돼요!**
        - 같은 날짜 데이터가 이미 저장된 경우, 덮어쓰기 버튼을 눌러주세요.
    - **Q. 뉴스가 너무 적게 나와요!**
        - 기사 검색수를 늘리거나, 장 마감 후에 다시 시도해보세요.

    ---
    ### 문의 및 피드백
    - 오류/건의사항은 개발자에게 직접 문의해 주세요.
    - [이메일: hellolk2000@gmail.com]
    """)

def read_google_sheet(worksheet_name=None):
    sheet = get_google_sheet()
    if not sheet:
        st.error("구글 시트 연결 실패")
        return None
    # 워크시트 목록 가져오기
    worksheet_list = sheet.worksheets()
    worksheet_names = [ws.title for ws in worksheet_list]
    if not worksheet_names:
        st.warning("구글 시트에 워크시트가 없습니다.")
        return None
    # 워크시트 선택
    if worksheet_name is None:
        worksheet_name = worksheet_names[-1]  # 기본값: 마지막 워크시트
    worksheet = sheet.worksheet(worksheet_name)
    data = worksheet.get_all_records()
    if not data:
        st.warning(f"{worksheet_name} 워크시트에 데이터가 없습니다.")
        return None
    import pandas as pd
    df = pd.DataFrame(data)
    return df, worksheet_names


