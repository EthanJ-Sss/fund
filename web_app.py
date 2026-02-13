# -*- coding: utf-8 -*-
"""
量化选基系统 - Web可视化界面
"""
import os
import sys
import json
from datetime import datetime
from threading import Thread

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, render_template, jsonify, request
from flask.json.provider import DefaultJSONProvider
from loguru import logger
import math


class CustomJSONProvider(DefaultJSONProvider):
    """自定义 JSON 序列化，处理 NaN 和 Infinity"""
    def default(self, obj):
        if isinstance(obj, float):
            if math.isnan(obj) or math.isinf(obj):
                return None
        return super().default(obj)

# 配置日志
logger.remove()
logger.add(sys.stderr, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")

app = Flask(__name__, template_folder='templates', static_folder='static')
app.json = CustomJSONProvider(app)


def clean_nan(obj):
    """清理数据中的 NaN 值"""
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

# 全局变量存储分析状态
analysis_status = {
    'running': False,
    'progress': 0,
    'message': '',
    'last_result': None
}


def get_collectors():
    """获取数据采集器实例"""
    from src.collector.akshare_collector import AKShareCollector
    from src.collector.eastmoney_collector import EastMoneyCollector
    from src.collector.alipay_filter import AlipayFundFilter
    return AKShareCollector(), EastMoneyCollector(), AlipayFundFilter()


def get_workflow():
    """获取工作流实例"""
    from src.workflow.fund_analysis import FundAnalysisWorkflow
    return FundAnalysisWorkflow()


def get_storage():
    """获取存储实例"""
    from src.storage.fund_storage import FundStorage
    return FundStorage()


@app.route('/')
def index():
    """首页"""
    return render_template('index.html')


@app.route('/api/stats')
def api_stats():
    """获取基金统计数据"""
    try:
        _, eastmoney, alipay_filter = get_collectors()
        import pandas as pd
        
        # 尝试从缓存加载
        storage = get_storage()
        cached = storage.load_fund_list('all', max_age_hours=24)
        
        if cached is not None and not cached.empty:
            df = cached
        else:
            # 获取基金列表
            fund_list = eastmoney.get_fund_list()
            
            if not fund_list:
                # 返回默认数据
                return jsonify({
                    'total': 0,
                    'purchasable': 0,
                    'categories': {},
                    'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'message': '正在获取数据...'
                })
            
            df = pd.DataFrame(fund_list)
            storage.save_fund_list(df, 'all')
        
        total = len(df)
        
        # 过滤支付宝可购
        filtered = alipay_filter.filter_purchasable(df)
        purchasable = len(filtered)
        
        # 分类统计
        categorized = alipay_filter.categorize_funds(filtered)
        categories = {}
        for fund_type, funds in categorized.items():
            if not funds.empty:
                categories[fund_type] = len(funds)
        
        return jsonify({
            'total': total,
            'purchasable': purchasable,
            'categories': categories,
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    except Exception as e:
        logger.error(f"获取统计数据失败: {e}")
        return jsonify({
            'total': 0,
            'purchasable': 0,
            'categories': {},
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'error': str(e)
        })


@app.route('/api/ranking')
def api_ranking():
    """获取基金业绩排名"""
    fund_type = request.args.get('type', 'stock')
    limit = int(request.args.get('limit', 20))
    
    try:
        _, eastmoney, _ = get_collectors()
        
        type_map = {
            'stock': 'gp',
            'mixed': 'hh',
            'bond': 'zq',
            'index': 'zs',
            'all': 'all'
        }
        
        ft = type_map.get(fund_type, 'gp')
        df = eastmoney.get_fund_performance_rank(fund_type=ft, page_size=limit)
        
        if df.empty:
            return jsonify({'funds': [], 'type': fund_type, 'message': '暂无数据'})
        
        # 转换为列表并清理 NaN 值
        funds = df.head(limit).to_dict('records')
        funds = clean_nan(funds)
        
        return jsonify({
            'funds': funds,
            'type': fund_type,
            'count': len(funds)
        })
    except Exception as e:
        logger.error(f"获取排名失败: {e}")
        return jsonify({'funds': [], 'type': fund_type, 'error': str(e)})


@app.route('/api/analyze/<fund_code>')
def api_analyze_fund(fund_code):
    """分析单只基金"""
    try:
        workflow = get_workflow()
        result = workflow.analyze_single_fund(fund_code)
        result = clean_nan(result)
        return jsonify(result)
    except Exception as e:
        logger.error(f"分析基金失败: {e}")
        return jsonify({'error': str(e), 'success': False}), 500


@app.route('/api/top_funds')
def api_top_funds():
    """获取推荐基金列表"""
    fund_type = request.args.get('type', 'all')
    limit = int(request.args.get('limit', 20))
    
    # 类型映射（前端用英文，存储用中文）
    type_map = {
        'stock': '股票型',
        'mixed': '混合型',
        'bond': '债券型',
        'index': '指数型',
        'all': 'all'
    }
    storage_type = type_map.get(fund_type, fund_type)
    
    try:
        storage = get_storage()
        df = storage.get_top_funds(storage_type, top_n=limit)
        
        if df.empty:
            return jsonify({'funds': [], 'message': '暂无数据，请先运行分析'})
        
        funds = df.to_dict('records')
        funds = clean_nan(funds)
        return jsonify({
            'funds': funds,
            'type': fund_type,
            'count': len(funds)
        })
    except Exception as e:
        logger.error(f"获取推荐基金失败: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/run_analysis', methods=['POST'])
def api_run_analysis():
    """运行量化选基分析"""
    global analysis_status
    
    if analysis_status['running']:
        return jsonify({'status': 'already_running', 'message': '分析正在进行中...'})
    
    def run_analysis_task():
        global analysis_status
        try:
            analysis_status['running'] = True
            analysis_status['progress'] = 10
            analysis_status['message'] = '正在获取基金列表...'
            
            workflow = get_workflow()
            
            analysis_status['progress'] = 30
            analysis_status['message'] = '正在分析基金数据...'
            
            result = workflow.run_full_analysis(
                fund_types=['股票型', '混合型', '指数型'],
                top_n=30,
                use_cache=True,
                save_results=True
            )
            
            analysis_status['progress'] = 100
            analysis_status['message'] = '分析完成!'
            analysis_status['last_result'] = {
                'elapsed_seconds': result.get('elapsed_seconds', 0),
                'statistics': result.get('statistics', {}),
                'complete_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            analysis_status['message'] = f'分析失败: {str(e)}'
            logger.error(f"分析失败: {e}")
        finally:
            analysis_status['running'] = False
    
    # 在后台线程中运行
    thread = Thread(target=run_analysis_task)
    thread.daemon = True
    thread.start()
    
    return jsonify({'status': 'started', 'message': '分析已开始'})


@app.route('/api/analysis_status')
def api_analysis_status():
    """获取分析状态"""
    return jsonify(analysis_status)


@app.route('/api/search')
def api_search():
    """搜索基金"""
    keyword = request.args.get('q', '').strip()
    if not keyword or len(keyword) < 2:
        return jsonify({'funds': []})
    
    try:
        _, eastmoney, _ = get_collectors()
        fund_list = eastmoney.get_fund_list()
        
        if not fund_list:
            return jsonify({'funds': []})
        
        # 搜索匹配
        results = []
        keyword_lower = keyword.lower()
        for fund in fund_list:
            code = fund.get('code', '')
            name = fund.get('name', '')
            if keyword_lower in code.lower() or keyword_lower in name.lower():
                results.append(fund)
                if len(results) >= 20:
                    break
        
        return jsonify({'funds': results})
    except Exception as e:
        return jsonify({'funds': [], 'error': str(e)})


if __name__ == '__main__':
    # 确保模板目录存在
    os.makedirs('templates', exist_ok=True)
    os.makedirs('static', exist_ok=True)
    
    print("\n" + "="*60)
    print("  量化选基系统 - Web可视化界面")
    print("="*60)
    print("\n  访问地址: http://127.0.0.1:8080")
    print("\n  按 Ctrl+C 停止服务器")
    print("="*60 + "\n")
    
    # 使用 127.0.0.1 而不是 0.0.0.0，避免防火墙问题
    app.run(host='127.0.0.1', port=8080, debug=True, threaded=True)
