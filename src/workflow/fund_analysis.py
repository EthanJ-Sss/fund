"""
基金分析工作流

整合数据采集、处理、评分的完整流程
"""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

from ..collector.akshare_collector import AKShareCollector
from ..collector.eastmoney_collector import EastMoneyCollector
from ..collector.alipay_filter import AlipayFundFilter
from ..processor.fund_processor import FundDataProcessor
from ..model.factors import FactorCalculator
from ..model.scoring_model import ScoringModel, get_scoring_config_for_type
from ..model.prefilter import PreFilter, ModeratePreFilter
from ..storage.fund_storage import FundStorage
from ..advisor.portfolio_advisor import PortfolioAdvisor


class FundAnalysisWorkflow:
    """
    基金分析工作流
    
    完整流程：
    1. 数据采集：获取基金列表和详情
    2. 数据过滤：筛选支付宝可购基金
    3. 指标计算：计算各类因子
    4. 预筛选：应用 4433 等规则
    5. 综合评分：多因子加权评分
    6. 结果存储：保存评分和报告
    """
    
    def __init__(
        self,
        akshare_collector: AKShareCollector = None,
        eastmoney_collector: EastMoneyCollector = None,
        alipay_filter: AlipayFundFilter = None,
        processor: FundDataProcessor = None,
        factor_calculator: FactorCalculator = None,
        scoring_model: ScoringModel = None,
        pre_filter: PreFilter = None,
        storage: FundStorage = None,
        max_workers: int = 5
    ):
        """
        初始化工作流
        
        Args:
            各组件实例（可选，使用默认实例）
            max_workers: 并发线程数
        """
        self.akshare = akshare_collector or AKShareCollector()
        self.eastmoney = eastmoney_collector or EastMoneyCollector()
        self.alipay_filter = alipay_filter or AlipayFundFilter()
        self.processor = processor or FundDataProcessor()
        self.factor_calc = factor_calculator or FactorCalculator(self.processor)
        self.scoring = scoring_model or ScoringModel()
        self.pre_filter = pre_filter or ModeratePreFilter()
        self.storage = storage or FundStorage()
        self.max_workers = max_workers
        self.benchmark_data = None  # 缓存基准数据
    
    def run_full_analysis(
        self,
        fund_types: List[str] = None,
        top_n: int = 50,
        use_cache: bool = True,
        save_results: bool = True,
        limit: int = 50  # 新增
    ) -> Dict:
        """
        运行完整分析流程
        
        Args:
            fund_types: 要分析的基金类型列表
            top_n: 每类保留前 N 名
            use_cache: 是否使用缓存
            save_results: 是否保存结果
            limit: 每类分析的最大数量
            
        Returns:
            分析结果字典
        """
        if fund_types is None:
            fund_types = ['股票型', '混合型', '指数型', '债券型']
        
        start_time = time.time()
        logger.info(f"开始全量分析，类型: {fund_types}, 每类最大分析数: {limit}")
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'top_funds': {},
            'statistics': {}
        }
        
        # 0. 提前获取基准数据 (sh000300)
        logger.info("[Step 0] 正在获取沪深300基准数据...")
        try:
            self.benchmark_data = self.akshare.get_index_daily("sh000300")
            if self.benchmark_data is not None and not self.benchmark_data.empty:
                logger.info(f"[Step 0] 基准数据获取成功，共 {len(self.benchmark_data)} 条记录")
            else:
                logger.warning("[Step 0] 基准数据为空")
        except Exception as e:
            logger.warning(f"[Step 0] 基准数据获取失败（不影响分析）: {e}")

        # 1. 获取基金列表
        logger.info("[Step 1] 正在获取基金列表...")
        all_funds = self._get_fund_list(use_cache)
        logger.info(f"[Step 1] 获取到 {len(all_funds)} 只基金")
        if not all_funds.empty:
            logger.info(f"[Step 1] 列名: {list(all_funds.columns)}")
        
        if all_funds.empty:
            logger.error("[Step 1] 无法获取基金列表，终止分析")
            return results

        # 2. 筛选支付宝可购
        logger.info("[Step 2] 正在筛选支付宝可购基金...")
        filtered_funds = self.alipay_filter.filter_purchasable(all_funds)
        logger.info(f"[Step 2] 支付宝可购基金: {len(filtered_funds)} 只")
        
        # 3. 按类型分类
        logger.info("[Step 3] 正在按类型分类...")
        categorized = self.alipay_filter.categorize_funds(filtered_funds)
        for cat_name, cat_df in categorized.items():
            logger.info(f"[Step 3]   {cat_name}: {len(cat_df)} 只")
        
        logger.info(f"[Step 4] 开始逐类型分析，待分析类型: {fund_types}")
        for fund_type in fund_types:
            if fund_type not in categorized or categorized[fund_type].empty:
                logger.warning(f"[Step 4] 类型 '{fund_type}' 没有可用基金，跳过")
                continue
            
            type_funds = categorized[fund_type]
            logger.info(f"[Step 4] === 开始处理 {fund_type}: 共 {len(type_funds)} 只 ===")
            
            # 分析该类型基金
            type_result = self._analyze_fund_type(
                type_funds, fund_type, top_n, use_cache, limit
            )
            
            results['top_funds'][fund_type] = type_result['top_funds']
            results['statistics'][fund_type] = type_result['statistics']
            
            # 保存该类型结果
            if save_results and type_result['scores_df'] is not None:
                logger.info(f"[Step 4] 正在保存 {fund_type} 评分结果...")
                self.storage.save_scores(
                    type_result['scores_df'],
                    fund_type=fund_type
                )
                logger.info(f"[Step 4] {fund_type} 评分结果已保存")
        
        # 完成
        elapsed = time.time() - start_time
        results['elapsed_seconds'] = elapsed
        results['end_time'] = datetime.now().isoformat()
        
        logger.info(f"分析完成，耗时 {elapsed:.1f} 秒")
        
        # 保存完整报告
        if save_results:
            self.storage.save_report(results, 'full_analysis')
        
        return results
    
    def _get_fund_list(self, use_cache: bool = True) -> pd.DataFrame:
        """获取基金列表"""
        # 尝试从缓存加载
        if use_cache:
            cached = self.storage.load_fund_list('all', max_age_hours=24)
            if cached is not None:
                return cached
        
        # 从天天基金获取（速度更快）
        try:
            fund_list = self.eastmoney.get_fund_list()
            if fund_list:
                df = pd.DataFrame(fund_list)
                self.storage.save_fund_list(df, 'all')
                return df
        except Exception as e:
            logger.warning(f"天天基金获取失败: {e}")
        
        # 备选：从 AKShare 获取
        try:
            df = self.akshare.get_all_funds()
            if not df.empty:
                self.storage.save_fund_list(df, 'all')
                return df
        except Exception as e:
            logger.error(f"AKShare 获取失败: {e}")
        
        return pd.DataFrame()
    
    def _analyze_fund_type(
        self,
        funds: pd.DataFrame,
        fund_type: str,
        top_n: int,
        use_cache: bool,
        limit: int = 50  # 新增 limit 参数，默认限制 50 只
    ) -> Dict:
        """
        分析单个类型的基金
        
        Args:
            funds: 该类型的基金列表
            fund_type: 基金类型
            top_n: 保留前 N 名
            use_cache: 是否使用缓存
            limit: 最大分析数量
            
        Returns:
            分析结果
        """
        result = {
            'top_funds': [],
            'scores_df': None,
            'statistics': {
                'total': len(funds),
                'analyzed': 0,
                'passed_prefilter': 0,
            }
        }
        
        # 限制分析数量（避免太慢）
        # 优先分析近期业绩较好的（假设 funds 已经包含业绩数据）
        # 如果 funds 中有 return_1y 字段，先排序
        if 'return_1y' in funds.columns:
            funds = funds.sort_values('return_1y', ascending=False)
            
        max_analyze = min(limit, len(funds))
        funds_to_analyze = funds.head(max_analyze)
        
        # 获取基金详情和计算因子
        funds_factors = {}
        funds_data = {}
        
        logger.info(f"开始获取 {fund_type} {len(funds_to_analyze)} 只基金详情...")
        
        # 使用线程池并发获取
        completed_count = 0
        failed_count = 0
        logger.info(f"[{fund_type}] 开始并发获取，线程数: {self.max_workers}")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {}
            
            for _, row in funds_to_analyze.iterrows():
                fund_code = str(row.get('基金代码', row.get('code', '')))
                if fund_code:
                    future = executor.submit(
                        self._get_fund_factors, fund_code, use_cache
                    )
                    futures[future] = fund_code
            
            logger.info(f"[{fund_type}] 已提交 {len(futures)} 个任务")
            
            for future in as_completed(futures):
                fund_code = futures[future]
                completed_count += 1
                    
                try:
                    factors, data = future.result()
                    if factors:
                        funds_factors[fund_code] = factors
                        funds_data[fund_code] = data
                    else:
                        failed_count += 1
                except Exception as e:
                    failed_count += 1
                    logger.warning(f"[{fund_type}] 基金 {fund_code} 分析异常: {e}")
                
                if completed_count % 5 == 0 or completed_count == len(futures):
                    logger.info(f"[{fund_type}] 进度: {completed_count}/{len(futures)} (成功: {len(funds_factors)}, 失败: {failed_count})")
        
        result['statistics']['analyzed'] = len(funds_factors)
        logger.info(f"[{fund_type}] 数据获取完成，成功: {len(funds_factors)}, 失败: {failed_count}")
        
        if not funds_factors:
            logger.warning(f"[{fund_type}] 没有任何基金成功获取因子，跳过后续步骤")
            return result
        
        # 预筛选
        logger.info(f"[{fund_type}] 开始预筛选...")
        prefilter_input = [
            {**funds_data.get(code, {}), **factors, 'fund_code': code}
            for code, factors in funds_factors.items()
        ]
        passed_funds, failed_funds = self.pre_filter.filter_batch(prefilter_input)
        result['statistics']['passed_prefilter'] = len(passed_funds)
        
        logger.info(f"[{fund_type}] 预筛选通过: {len(passed_funds)} / {len(prefilter_input)}")
        
        # 综合评分
        passed_codes = {f['fund_code'] for f in passed_funds}
        passed_factors = {
            code: factors for code, factors in funds_factors.items()
            if code in passed_codes
        }
        
        if passed_factors:
            logger.info(f"[{fund_type}] 开始综合评分，共 {len(passed_factors)} 只...")
            config = get_scoring_config_for_type(fund_type)
            type_scoring = ScoringModel(config)
            
            scores_df = type_scoring.batch_score(passed_factors)
            
            # 添加基金名称等信息
            code_to_name = {}
            for _, row in funds_to_analyze.iterrows():
                code = str(row.get('基金代码', row.get('code', '')))
                name = row.get('基金简称', row.get('name', ''))
                code_to_name[code] = name
            
            scores_df['fund_name'] = scores_df['fund_code'].map(code_to_name)
            scores_df['fund_type'] = fund_type
            
            result['scores_df'] = scores_df
            
            # 取前 N 名
            top_df = scores_df.head(top_n)
            result['top_funds'] = top_df.to_dict('records')
            
            logger.info(f"[{fund_type}] 评分完成，Top {top_n} 基金已生成")
            if not top_df.empty:
                logger.info(f"[{fund_type}] 最高分: {top_df.iloc[0].get('total_score', 0):.1f}, 最低分: {top_df.iloc[-1].get('total_score', 0):.1f}")
        else:
            logger.warning(f"[{fund_type}] 没有基金通过预筛选，无评分结果")
        
        return result
    
    def _get_fund_factors(
        self, 
        fund_code: str,
        use_cache: bool = True
    ) -> Tuple[Optional[Dict], Optional[Dict]]:
        """
        获取单只基金的因子
        
        Returns:
            (因子字典, 基金数据字典)
        """
        # 获取净值数据
        nav_data = None
        if use_cache:
            nav_data = self.storage.load_nav_data(fund_code)
        
        if nav_data is None:
            try:
                nav_data = self.akshare.get_fund_nav_history(fund_code)
                if nav_data is not None and not nav_data.empty:
                    self.storage.save_nav_data(fund_code, nav_data)
            except Exception as e:
                logger.warning(f"[{fund_code}] 获取净值失败: {e}")
        
        if nav_data is None or nav_data.empty:
            logger.warning(f"[{fund_code}] 净值数据为空，跳过")
            return None, None
        
        # 获取基金基本信息
        fund_info = None
        current_holdings = None
        
        # 优先从 EastMoney 获取详细信息（包含经理、持仓等）
        try:
            em_detail = self.eastmoney.get_fund_detail(fund_code)
            if em_detail:
                if not fund_info:
                    fund_info = em_detail
                else:
                    fund_info.update(em_detail)
                    
                # 提取持仓信息
                if 'holdings' in em_detail and em_detail['holdings']:
                    current_holdings = pd.DataFrame(em_detail['holdings'])
                    if 'gdmc' in current_holdings.columns:
                        current_holdings['股票名称'] = current_holdings['gdmc']
                    if 'bzdm' in current_holdings.columns:
                        current_holdings['股票代码'] = current_holdings['bzdm']
                    if 'jzbl' in current_holdings.columns:
                        current_holdings['占净值比例'] = current_holdings['jzbl']
        except Exception as e:
            logger.warning(f"[{fund_code}] EastMoney 详情获取失败: {e}")

        if use_cache and not fund_info:
            fund_info = self.storage.load_fund_info(fund_code)
        
        if fund_info is None:
            try:
                fund_info = self.akshare.get_fund_basic_info(fund_code)
                if fund_info:
                    self.storage.save_fund_info(fund_code, fund_info)
            except Exception as e:
                logger.warning(f"[{fund_code}] AKShare 基金信息获取失败: {e}")
        
        # 如果还没获取到持仓，尝试从 AKShare 获取
        if current_holdings is None:
            try:
                current_holdings = self.akshare.get_fund_holdings(fund_code)
            except Exception as e:
                pass  # 持仓非必需，静默跳过

        # 构建基金数据
        fund_data = {
            'fund_code': fund_code,
            'scale': self._extract_scale(fund_info),
            'tenure_years': self._extract_tenure(fund_info),
            'manager_tenure': self._extract_manager_tenure(fund_info),
        }
        
        # 计算因子
        try:
            factors = self.factor_calc.calculate_all_factors(
                nav_data=nav_data,
                fund_info=fund_data,
                benchmark_data=self.benchmark_data,
                current_holdings=current_holdings
            )
            
            # 合并基金数据到因子
            factors.update(fund_data)
            
            return factors, fund_data
            
        except Exception as e:
            logger.warning(f"[{fund_code}] 计算因子失败: {e}")
            return None, None
    
    def _extract_scale(self, fund_info: Dict) -> float:
        """提取基金规模"""
        if not fund_info:
            return 0.0
        
        scale = fund_info.get('最新规模',
                fund_info.get('基金规模',
                fund_info.get('scale', 0)))
        if isinstance(scale, str):
            try:
                scale = float(scale.replace('亿元', '').replace('亿', ''))
            except:
                scale = 0.0
        
        return float(scale) if scale else 0.0
    
    def _extract_tenure(self, fund_info: Dict) -> float:
        """提取基金成立年限"""
        if not fund_info:
            return 0.0
        
        establish_date = fund_info.get('成立时间',
                         fund_info.get('成立日期',
                         fund_info.get('establish_date', '')))
        if establish_date:
            try:
                if isinstance(establish_date, str):
                    from datetime import datetime
                    est = datetime.strptime(establish_date[:10], '%Y-%m-%d')
                    years = (datetime.now() - est).days / 365
                    return years
            except:
                pass
        
        return 0.0
    
    def _extract_manager_tenure(self, fund_info: Dict) -> float:
        """提取基金经理任职年限"""
        if not fund_info:
            return 0.0
        
        # 优先解析 managers 列表（来自 EastMoney）
        managers = fund_info.get('managers')
        if managers and isinstance(managers, list):
            max_tenure = 0.0
            for mgr in managers:
                work_time = mgr.get('workTime', '')
                if work_time:
                    try:
                        import re
                        years_match = re.search(r'(\d+)年', work_time)
                        days_match = re.search(r'(\d+)天', work_time)
                        
                        years = int(years_match.group(1)) if years_match else 0
                        days = int(days_match.group(1)) if days_match else 0
                        
                        tenure = years + days / 365
                        if tenure > max_tenure:
                            max_tenure = tenure
                    except:
                        pass
            if max_tenure > 0:
                return max_tenure

        tenure = fund_info.get('任职时间',
                 fund_info.get('manager_tenure',
                 fund_info.get('任期', 0)))
        
        if isinstance(tenure, str):
            try:
                import re
                years_match = re.search(r'(\d+)年', tenure)
                days_match = re.search(r'(\d+)天', tenure)
                
                years = int(years_match.group(1)) if years_match else 0
                days = int(days_match.group(1)) if days_match else 0
                
                return years + days / 365
            except:
                return 0.0
        
        # 如果没有任职时间，用成立时间的一半作为粗略估计
        if not tenure and fund_info:
            establish_date = fund_info.get('成立时间',
                             fund_info.get('成立日期', ''))
            if establish_date:
                try:
                    from datetime import datetime
                    est = datetime.strptime(str(establish_date)[:10], '%Y-%m-%d')
                    fund_years = (datetime.now() - est).days / 365
                    return min(fund_years * 0.5, 5.0)
                except:
                    pass
        
        return float(tenure) if tenure else 0.0
    
    def analyze_single_fund(
        self, 
        fund_code: str,
        use_cache: bool = True
    ) -> Dict:
        """
        分析单只基金
        
        Args:
            fund_code: 基金代码
            use_cache: 是否使用缓存
            
        Returns:
            分析结果
        """
        logger.info(f"开始分析基金: {fund_code}")
        
        # 获取因子
        factors, fund_data = self._get_fund_factors(fund_code, use_cache)
        
        if not factors:
            return {
                'fund_code': fund_code,
                'success': False,
                'error': '无法获取基金数据'
            }
        
        # 预筛选
        prefilter_input = {**fund_data, **factors}
        passed, details = self.pre_filter.filter_single(prefilter_input, return_details=True)
        
        # 评分
        score_result = self.scoring.calculate_total_score(factors)
        
        # 获取建议
        recommendation = self.scoring.get_recommendation(
            score_result['total_score'], factors
        )
        
        return {
            'fund_code': fund_code,
            'success': True,
            'factors': factors,
            'prefilter_passed': passed,
            'prefilter_details': details,
            'score': score_result,
            'recommendation': recommendation
        }
    
    def get_top_recommendations(
        self,
        fund_type: str = 'all',
        top_n: int = 10
    ) -> List[Dict]:
        """
        获取推荐基金列表
        
        Args:
            fund_type: 基金类型
            top_n: 数量
            
        Returns:
            推荐基金列表
        """
        scores_df = self.storage.get_top_funds(fund_type, top_n)
        
        if scores_df.empty:
            return []
        
        recommendations = []
        if isinstance(scores_df, pd.DataFrame):
            # 将 DataFrame 转换为字典列表
            records = scores_df.to_dict('records')
            for row in records:
                rec = {
                    'fund_code': row.get('fund_code', ''),
                    'fund_name': row.get('fund_name', ''),
                    'fund_type': row.get('fund_type', fund_type),
                    'total_score': row.get('total_score', 0),
                    'grade': self.scoring.get_score_grade(row.get('total_score', 0)),
                    'rank': row.get('rank', 0),
                }
                recommendations.append(rec)
        
        return recommendations

    def get_portfolio_recommendation(self, risk_level: str = 'balanced') -> Dict:
        """
        获取投资组合建议
        
        Args:
            risk_level: 风险偏好 (conservative/balanced/aggressive)
            
        Returns:
            组合建议字典
        """
        advisor = PortfolioAdvisor()
        
        # 获取各类型排名靠前的基金
        top_funds = {}
        # 注意：这里使用 alipay_filter 中的分类名称
        # 如果需要映射，请确保一致
        for f_type in ['股票型', '混合型', '指数型', '债券型']:
            # get_top_recommendations 返回的是字典列表
            top_funds[f_type] = self.get_top_recommendations(f_type, top_n=20)
            
        return advisor.suggest_portfolio(top_funds, risk_level)



# 创建全局实例
fund_analysis_workflow = FundAnalysisWorkflow()
