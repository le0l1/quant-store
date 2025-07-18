<requirement>
使用python进行接口数据爬取，并输出到 dataframe，打印出来

接口地址: https://www.jisilu.cn/webapi/cb/list/

打印输出需要满足以下规则

1. 剔除 bond_nm 包含 `退` 字的行
2. 剔除  "price_tips": "待上市"
3. 剔除 "icons"下有R的 和 有O的  
```json 
"icons": {
    "R": ""
}

"icons": {
    "O": ""
}
```
4. sprice > 3
5. 根据 dblow 进行 升序排序，输出最小的 top 20

</requirement>


<api-response>
{
    "code": 200,
    "msg": "",
    "data": [
        {
            "bond_id": "404004",
            "bond_nm": "汇车退债",
            "bond_py": "hctz",
            "price": 35.837,
            "increase_rt": 0,
            "stock_id": "400245",
            "stock_nm": "汇车5",
            "stock_py": "hc5",
            "sprice": 0.08,
            "sincrease_rt": -11.11,
            "pb": 0.02,
            "convert_price": 0.12,
            "convert_value": 66.67,
            "convert_dt": 0,
            "premium_rt": -46.25,
            "dblow": -10.41,
            "sw_cd": "280302",
            "market_cd": "sb",
            "btype": "C",
            "list_dt": "2024-11-01",
            "t_flag": [],
            "owned": 0,
            "hold": 0,
            "bond_value": null,
            "rating_cd": "A",
            "option_value": null,
            "put_convert_price": 0.08,
            "force_redeem_price": 0.16,
            "convert_amt_ratio": 311.1,
            "fund_rt": null,
            "maturity_dt": "2026-08-18",
            "year_left": 1.085,
            "curr_iss_amt": 20.65,
            "volume": 0,
            "svolume": 0,
            "turnover_rt": 0,
            "ytm_rt": 194.37,
            "put_ytm_rt": null,
            "noted": 0,
            "last_time": "10:43:06",
            "qstatus": "00",
            "sqflag": "Y",
            "pb_flag": "Y",
            "adj_cnt": 5,
            "adj_scnt": "5",
            "convert_price_valid": "Y",
            "convert_price_tips": "转股价下修5次，成功下修5次",
            "convert_cd_tip": "2021-02-24 开始转股",
            "ref_yield_info": "计算使用1.1年期 评级为A 债参考YTM：6.9003",
            "adjusted": "N",
            "orig_iss_amt": 33.7,
            "price_tips": "全价：35.837 最后更新：10:43:06",
            "redeem_dt": null,
            "real_force_redeem_price": null,
            "option_tip": "",
            "after_next_put_dt": 1,
            "icons": [],
            "is_min_price": 0,
            "blocked": 0,
            "putting": "N",
            "force_redeem_price_tip": "",
            "notes": null
        }
    ],
    "formula": [],
    "info": {
        "date": "2025-07-18"
    },
    "annual": "2024",
    "prompt": ""
}
</api-responose>

