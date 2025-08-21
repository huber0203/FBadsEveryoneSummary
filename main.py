import os
import requests
import json
import logging
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI 應用程式初始化
app = FastAPI(
    title="Meta Ads Reporter API",
    version="1.0.0",
    description="Meta 廣告數據報告 API"
)

# 加入 CORS 支援
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://xlearn.tw", "http://localhost:*"],  # 明確指定允許的網域
    allow_credentials=True,
    allow_methods=["POST", "OPTIONS"],
    allow_headers=["*"],
)

# 請求模型
class AdsReportRequest(BaseModel):
    access_token: str
    date_start: str  # YYYY-MM-DD
    date_end: str    # YYYY-MM-DD

# 回應模型
class AdsReportResponse(BaseModel):
    success: bool
    message: str
    data: Optional[dict] = None
    error: Optional[str] = None

class MetaAdsReporter:
    def __init__(self, access_token):
        self.access_token = access_token
        self.base_url = "https://graph.facebook.com/v18.0"
        self.logger = logging.getLogger(f"{__name__}.MetaAdsReporter")
        
        # 分級規則定義
        self.grade_rules = {
            "課程": "R", "求職": "R", "懶人包": "N", "素材": "N", "優惠": "R",
            "接案": "R", "諮詢": "R", "小遊戲": "C", "職能講座": "SR", "職能工作坊": "SR",
            "軟實力講座": "R", "軟實力工作坊": "R", "培訓營": "SR", "互動測驗": "C",
            "實習": "N", "自來客": "SSR", "社群互動": "C"
        }
        
    def get_ad_accounts(self):
        url = f"{self.base_url}/me/adaccounts"
        params = {
            'access_token': self.access_token,
            'fields': 'id,name,account_status'
        }
        
        self.logger.info(f"🔍 Fetching ad accounts from Meta API")
        
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            data = response.json().get('data', [])
            self.logger.info(f"✅ Found {len(data)} ad accounts")
            return data
        else:
            error_data = response.json().get('error', {})
            error_code = error_data.get('code', 'Unknown')
            error_message = error_data.get('message', response.text)
            
            if error_code == 190:
                raise Exception(f"Token 無效或已過期: {error_message}")
            elif error_code == 200:
                raise Exception(f"Token 權限不足: {error_message}")
            else:
                raise Exception(f"Meta API 錯誤 (Code: {error_code}): {error_message}")
    
    def get_ads_insights(self, ad_account_id, date_start, date_end):
        url = f"{self.base_url}/{ad_account_id}/insights"
        
        params = {
            'access_token': self.access_token,
            'level': 'ad',
            'fields': 'ad_name,ad_id,spend,actions,cost_per_action_type',
            'time_range': json.dumps({
                'since': date_start,
                'until': date_end
            }),
            'limit': 500
        }
        
        self.logger.info(f"📊 Fetching insights for account {ad_account_id}")
        
        all_ads = []
        
        while True:
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                ads_data = data.get('data', [])
                all_ads.extend(ads_data)
                
                if 'paging' in data and 'next' in data['paging']:
                    url = data['paging']['next']
                    params = {}
                else:
                    break
            else:
                self.logger.warning(f"Failed to get insights for {ad_account_id}")
                break
        
        self.logger.info(f"✅ Total ads fetched: {len(all_ads)}")
        return all_ads
    
    def calculate_leads_and_cpl(self, ad_data):
        spend = float(ad_data.get('spend', 0))
        leads = 0
        cpl = 0
        
        actions = ad_data.get('actions', [])
        
        # 優先順序：
        # 1. offsite_conversion.fb_pixel_custom (包含 Submit 或 SurveyCake)
        # 2. lead
        
        # 先檢查是否有 custom conversion
        custom_conversion_found = False
        for action in actions:
            action_type = action.get('action_type', '')
            
            # 檢查 custom conversion
            if action_type == 'offsite_conversion.fb_pixel_custom':
                # 可能需要檢查 action_destination 或其他欄位來確認是否為 Submit 類型
                leads = int(action.get('value', 0))
                custom_conversion_found = True
                self.logger.debug(f"Found custom conversion: {leads} leads")
                break
        
        # 如果沒有 custom conversion，才找 lead
        if not custom_conversion_found:
            for action in actions:
                action_type = action.get('action_type', '')
                
                if action_type == 'lead':
                    leads = int(action.get('value', 0))
                    self.logger.debug(f"Found standard lead: {leads} leads")
                    break
        
        # 如果還是沒找到，檢查其他 lead 相關的 action types
        if leads == 0:
            for action in actions:
                action_type = action.get('action_type', '')
                
                if action_type in [
                    'offsite_conversion.fb_pixel_lead',
                    'onsite_conversion.lead_grouped',
                    'leadgen_grouped'
                ] or ('lead' in action_type.lower() and action_type != 'lead'):
                    leads += int(action.get('value', 0))
                    self.logger.debug(f"Found other lead type {action_type}: {action.get('value', 0)} leads")
        
        # 如果完全沒有找到 lead，嘗試從 cost_per_action_type 推算
        if leads == 0:
            cost_per_actions = ad_data.get('cost_per_action_type', [])
            for cpa in cost_per_actions:
                action_type = cpa.get('action_type', '')
                
                # 同樣的優先順序
                if action_type == 'offsite_conversion.fb_pixel_custom':
                    cpl_value = float(cpa.get('value', 0))
                    if cpl_value > 0:
                        leads = int(spend / cpl_value)
                        self.logger.debug(f"Calculated from custom CPL: {leads} leads")
                        break
            
            # 如果沒有 custom，找 lead
            if leads == 0:
                for cpa in cost_per_actions:
                    if cpa.get('action_type', '') == 'lead':
                        cpl_value = float(cpa.get('value', 0))
                        if cpl_value > 0:
                            leads = int(spend / cpl_value)
                            self.logger.debug(f"Calculated from standard CPL: {leads} leads")
                            break
        
        # 計算 CPL
        if leads > 0:
            cpl = spend / leads
        
        return leads, int(cpl)
    
    def parse_ad_name(self, ad_name):
        try:
            parts = ad_name.split('/')
            
            if len(parts) < 3:
                return None
            
            page_name = parts[0]
            middle_part = parts[1]
            
            if '_' in middle_part:
                field_and_type = middle_part.split('_')
                field = field_and_type[0]
                
                if '-' in field_and_type[1]:
                    ad_type = field_and_type[1].split('-')[0]
                else:
                    ad_type = field_and_type[1]
            else:
                field = middle_part
                ad_type = "未分類"
            
            # 檢查是否有明確的等級標記（如：課程N, 求職SR）
            grade = None
            grade_markers = ['SSR', 'SR', 'R', 'N', 'C', 'D']
            
            for marker in grade_markers:
                if ad_type.endswith(marker):
                    # 提取等級並移除等級標記
                    grade = marker
                    ad_type = ad_type[:-len(marker)]
                    break
            
            # 如果沒有明確標記，使用預設規則
            if grade is None:
                grade = self.grade_rules.get(ad_type, "D")
            
            employee_part = parts[-1]
            employees = employee_part.split('+') if '+' in employee_part else [employee_part]
            
            return {
                'page_name': page_name,
                'field': field,
                'ad_type': ad_type,
                'grade': grade,
                'employees': employees,
                'employee_key': '+'.join(sorted(employees))
            }
            
        except Exception:
            return None
    
    def generate_employee_summary(self, ads_data):
        employee_stats = {}
        
        for ad in ads_data:
            parsed = self.parse_ad_name(ad['ad_name'])
            
            if not parsed:
                continue
            
            employee_key = parsed['employee_key']
            
            if employee_key not in employee_stats:
                employee_stats[employee_key] = {
                    '員工': employee_key,
                    'SSR等級花費': {},
                    'SR等級花費': {},
                    'R等級花費': {},
                    'N等級花費': {},
                    'C等級花費': {},
                    'D等級花費': {}
                }
            
            grade = parsed['grade']
            field = parsed['field']
            grade_key = f"{grade}等級花費"
            
            if field not in employee_stats[employee_key][grade_key]:
                employee_stats[employee_key][grade_key][field] = {
                    'total_spend': 0,
                    'total_leads': 0,
                    'ads_count': 0
                }
            
            field_stats = employee_stats[employee_key][grade_key][field]
            field_stats['total_spend'] += ad['spend']
            field_stats['total_leads'] += ad['leads']
            field_stats['ads_count'] += 1
        
        summary_list = []
        
        for employee_key, stats in employee_stats.items():
            formatted_stats = {'員工': stats['員工']}
            
            for grade in ['SSR等級花費', 'SR等級花費', 'R等級花費', 'N等級花費', 'C等級花費', 'D等級花費']:
                grade_data = []
                
                if grade in stats and stats[grade]:
                    for field, field_stats in stats[grade].items():
                        if field_stats['ads_count'] > 0:
                            avg_cpl = int(field_stats['total_spend'] / field_stats['total_leads']) if field_stats['total_leads'] > 0 else 0
                            
                            grade_data.append(
                                f"{field},average_cpl:{avg_cpl},total_spend:{int(field_stats['total_spend'])},"
                                f"total_leads:{field_stats['total_leads']},ads_count:{field_stats['ads_count']}"
                            )
                
                formatted_stats[grade] = grade_data if grade_data else []
            
            summary_list.append(formatted_stats)
        
        return summary_list
    
    def generate_report(self, date_start, date_end):
        self.logger.info(f"🚀 Starting report generation: {date_start} to {date_end}")
        
        ad_accounts = self.get_ad_accounts()
        
        if not ad_accounts:
            return self._empty_report(date_start, date_end)
        
        all_ads_data = []
        
        for account in ad_accounts:
            account_id = account['id']
            account_name = account.get('name', 'Unknown')
            
            ads_insights = self.get_ads_insights(account_id, date_start, date_end)
            
            for ad_data in ads_insights:
                leads, cpl = self.calculate_leads_and_cpl(ad_data)
                
                ad_info = {
                    'account_name': account_name,
                    'account_id': account_id,
                    'ad_name': ad_data.get('ad_name', 'Unknown'),
                    'ad_id': ad_data.get('ad_id', ''),
                    'spend': int(float(ad_data.get('spend', 0))),
                    'leads': leads,
                    'cpl': int(cpl) if cpl > 0 else 0
                }
                
                all_ads_data.append(ad_info)
        
        total_spend = sum(ad['spend'] for ad in all_ads_data)
        total_leads = sum(ad['leads'] for ad in all_ads_data)
        avg_cpl = total_spend / total_leads if total_leads > 0 else 0
        
        by_account = {}
        for ad in all_ads_data:
            account_name = ad['account_name']
            if account_name not in by_account:
                by_account[account_name] = {
                    'account_id': ad['account_id'],
                    'total_spend': 0,
                    'total_leads': 0,
                    'ads_count': 0
                }
            by_account[account_name]['total_spend'] += ad['spend']
            by_account[account_name]['total_leads'] += ad['leads']
            by_account[account_name]['ads_count'] += 1
        
        for account in by_account.values():
            account['average_cpl'] = int(
                account['total_spend'] / account['total_leads']
            ) if account['total_leads'] > 0 else 0
            account['total_spend'] = int(account['total_spend'])
        
        employee_summary = self.generate_employee_summary(all_ads_data)
        
        self.logger.info(f"📈 Report Summary: Spend ${int(total_spend)}, Leads {total_leads}, CPL ${int(avg_cpl)}")
        
        return {
            'report_generated_at': datetime.now().isoformat(),
            'period': {
                'start_date': date_start,
                'end_date': date_end
            },
            'summary': {
                'total_spend': int(total_spend),
                'total_leads': total_leads,
                'average_cpl': int(avg_cpl),
                'total_ads': len(all_ads_data),
                'total_accounts': len(by_account)
            },
            'by_account': by_account,
            'employee_summary': employee_summary,
            'ads_detail': all_ads_data
        }
    
    def _empty_report(self, date_start, date_end):
        return {
            'report_generated_at': datetime.now().isoformat(),
            'period': {
                'start_date': date_start,
                'end_date': date_end
            },
            'summary': {
                'total_spend': 0,
                'total_leads': 0,
                'average_cpl': 0,
                'total_ads': 0,
                'total_accounts': 0
            },
            'by_account': {},
            'employee_summary': [],
            'ads_detail': []
        }

@app.post("/report", response_model=AdsReportResponse)
async def generate_ads_report(request: AdsReportRequest):
    """
    生成 Meta 廣告報告
    
    Args:
        request: 包含 access_token, date_start, date_end
    
    Returns:
        JSON 格式的廣告報告
    """
    try:
        # 驗證日期格式
        datetime.strptime(request.date_start, '%Y-%m-%d')
        datetime.strptime(request.date_end, '%Y-%m-%d')
        
        # 生成報告
        reporter = MetaAdsReporter(request.access_token)
        report = reporter.generate_report(request.date_start, request.date_end)
        
        return AdsReportResponse(
            success=True,
            message=f"成功生成報告，共找到 {report['summary']['total_ads']} 個廣告",
            data=report
        )
    
    except ValueError:
        return AdsReportResponse(
            success=False,
            message="日期格式錯誤",
            error="請使用 YYYY-MM-DD 格式"
        )
    except Exception as e:
        error_message = str(e)
        if "Token 無效或已過期" in error_message:
            return AdsReportResponse(
                success=False,
                message="Token 驗證失敗",
                error="Access Token 無效或已過期"
            )
        elif "權限不足" in error_message:
            return AdsReportResponse(
                success=False,
                message="權限不足",
                error="Token 缺少 ads_read 權限"
            )
        else:
            return AdsReportResponse(
                success=False,
                message="生成報告失敗",
                error=error_message
            )

# Zeabur 會自動尋找 app 變數
# 本機測試：uvicorn main:app --reload --host 0.0.0.0 --port 8000
