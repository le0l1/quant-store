# 量化数据存储系统 - DuckDB版本

基于DuckDB的量化数据存储系统，支持每日自动更新可转债数据。

## 功能特性

- 🚀 **高性能存储**: 使用DuckDB进行高效的数据存储
- 📊 **全量数据**: 保留所有原始数据，不进行过滤
- 🔄 **增量更新**: 智能检测数据变化，避免重复更新
- 📅 **定时任务**: 支持工作日自动更新数据
- 📝 **操作日志**: 完整的更新日志记录
- 💾 **数据保留**: 保留所有历史数据，不自动清理

## 系统架构

```
dataset/
├── update.py          # 主数据更新脚本
├── scheduler.py       # 定时任务调度器
├── view_data.py       # 数据库内容查看工具
├── manage_scheduler.sh # 调度器管理脚本
├── check_timezone.py  # 时区检查脚本
├── pyproject.toml     # 项目配置和依赖管理
└── README.md         # 说明文档
```

## 安装和配置

### 1. 安装依赖

```bash
# 使用uv安装依赖
uv sync

# 或者激活虚拟环境后安装
uv venv
source .venv/bin/activate  # Linux/Mac
# 或 .venv\Scripts\activate  # Windows
uv pip install -e .
```

### 2. 设置环境变量

```bash
# 设置集思录Cookie
export JISILU_COOKIE='你的Cookie字符串'
```

获取Cookie步骤：
1. 打开浏览器，访问 https://www.jisilu.cn/
2. 登录你的账户
3. 按F12打开开发者工具
4. 切换到Network标签页
5. 刷新页面，找到对 www.jisilu.cn 的请求
6. 在请求头中找到Cookie字段并复制

### 3. 初始化数据库

```bash
uv run python update.py
```

## 使用方法

### 手动更新数据

```bash
# 更新可转债数据
uv run python update.py
```

### 启动定时任务

#### 前台运行
```bash
# 启动自动更新调度器（前台运行）
uv run python scheduler.py
```

#### 后台运行
```bash
# 使用管理脚本（推荐）
./manage_scheduler.sh start    # 启动
./manage_scheduler.sh stop     # 停止
./manage_scheduler.sh restart  # 重启
./manage_scheduler.sh status   # 查看状态
./manage_scheduler.sh logs     # 查看日志

# 或使用nohup
nohup uv run python scheduler.py > scheduler.log 2>&1 &
```

调度器会在以下时间自动执行：
- 工作日 15:15 (北京时间) - 更新可转债数据

### 时区设置

如果服务器不在东八时区，系统会自动设置时区为北京时间：

```bash
# 检查时区设置
uv run python check_timezone.py

# 系统级别设置时区（可选）
sudo timedatectl set-timezone Asia/Shanghai
```

### 查看数据

使用内置的查看工具：

```bash
# 查看数据库内容
uv run python view_data.py

# 查看指定数据库文件
uv run python view_data.py --db-path my_data.duckdb

# 导出数据到CSV
uv run python view_data.py --export

# 导出到指定目录
uv run python view_data.py --export --output-dir my_exports
```

或者使用DuckDB命令行工具：

```bash
# 使用DuckDB命令行工具
duckdb quant_data.duckdb

# 查看表结构
DESCRIBE convertible_bonds;

# 查看最新数据
SELECT * FROM convertible_bonds WHERE update_date = (SELECT MAX(update_date) FROM convertible_bonds) LIMIT 20;

# 查看更新日志
SELECT * FROM update_logs ORDER BY created_at DESC LIMIT 10;
```

## 数据处理

系统保留所有原始数据，不进行任何过滤：

- **全量存储**: 获取到的所有可转债数据都会存储到数据库
- **原始格式**: 保持API返回的原始数据格式
- **历史保留**: 所有历史数据都会保留，不会自动清理

## 数据库结构

### convertible_bonds 表

| 字段名 | 类型 | 说明 |
|--------|------|------|
| id | INTEGER | 主键ID |
| bond_id | VARCHAR | 债券代码 |
| bond_nm | VARCHAR | 债券名称 |
| price | DECIMAL(10,4) | 当前价格 |
| sprice | DECIMAL(10,4) | 转股价 |
| dblow | DECIMAL(10,4) | 双低值 |
| curr_iss_amt | DECIMAL(15,2) | 剩余规模 |
| rating_cd | VARCHAR(10) | 评级 |
| premium_rt | DECIMAL(10,4) | 溢价率 |
| increase_rt | DECIMAL(10,4) | 涨跌幅 |
| price_tips | VARCHAR(100) | 价格提示 |
| icons | JSON | 图标标记 |
| update_date | DATE | 更新日期 |
| data_hash | VARCHAR(64) | 数据哈希值 |
| created_at | TIMESTAMP | 创建时间 |
| updated_at | TIMESTAMP | 更新时间 |

### update_logs 表

| 字段名 | 类型 | 说明 |
|--------|------|------|
| id | INTEGER | 主键ID |
| table_name | VARCHAR(50) | 表名 |
| update_date | DATE | 更新日期 |
| records_count | INTEGER | 记录数量 |
| status | VARCHAR(20) | 状态 |
| error_message | TEXT | 错误信息 |
| execution_time_ms | INTEGER | 执行时间(毫秒) |
| created_at | TIMESTAMP | 创建时间 |

## 数据更新机制

- **增量更新**: 每天只更新一次数据，避免重复更新
- **变化检测**: 使用数据哈希值检测数据是否发生变化
- **强制更新**: 可以使用 `--force` 参数强制更新数据

## 日志文件

- `data_update.log`: 数据更新日志
- `scheduler.log`: 调度器运行日志

## 性能优化

1. **索引优化**: 在关键字段上创建索引
2. **数据哈希**: 使用哈希值检测数据变化
3. **增量更新**: 只更新变化的数据
4. **数据保留**: 保留所有历史数据供分析使用

## 故障排除

### 常见问题

1. **依赖安装问题**
   - 确保已安装uv: `pip install uv`
   - 使用 `uv sync` 安装依赖
   - 如果遇到权限问题，使用 `uv venv` 创建虚拟环境

2. **Cookie错误**
   - 确保设置了正确的 `JISILU_COOKIE` 环境变量
   - 检查Cookie是否过期

3. **数据库连接失败**
   - 检查文件权限
   - 确保磁盘空间充足

4. **数据更新失败**
   - 检查网络连接
   - 查看日志文件获取详细错误信息

### 调试模式

```bash
# 强制更新数据（忽略重复检查）
uv run python update.py --force

# 指定数据库路径
uv run python update.py --db-path my_data.duckdb
```

## 扩展功能

### 添加新的数据源

1. 在 `QuantDataManager` 类中添加新的数据获取方法
2. 创建对应的数据表结构
3. 实现数据处理逻辑
4. 添加到定时任务中

### 自定义数据处理

修改 `process_raw_data` 方法中的处理逻辑，添加自定义的数据处理规则。

## 开发环境

### 代码格式化

```bash
# 格式化代码
uv run black .

# 检查代码风格
uv run flake8 .
```

### 运行测试

```bash
# 运行测试
uv run pytest
```

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request来改进这个项目。 