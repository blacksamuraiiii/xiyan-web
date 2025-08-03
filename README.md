# 🎯 析言数据分析助手

<div align="center">

![析言数据分析助手](https://img.shields.io/badge/析言-数据分析助手-blue?style=for-the-badge&logo=data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHZpZXdCb3g9IjAgMCAyNCAyNCIgZmlsbD0ibm9uZSIgc3Ryb2tlPSJjdXJyZW50Q29sb3IiIHN0cm9rZS13aWR0aD0iMiIgc3Ryb2tlLWxpbmVjYXA9InJvdW5kIiBzdHJva2UtbGluZWpvaW49InJvdW5kIj48cGF0aCBkPSJNOSAxM2w0IDQgOC04Ii8+PHBhdGggZD0iTTIxIDEyYzAgMS4xLS45IDItMiAySDVjLTEuMSAwLTItLjktMi0yVjVjMC0xLjEuOS0yIDItMmgxNGMxLjEgMCAyIC45IDIgMnoiLz48L3N2Zz4=)
![版本](https://img.shields.io/badge/版本-v0.3.1-green?style=for-the-badge)
![许可证](https://img.shields.io/badge/许可证-MIT-blue?style=for-the-badge)
![Python](https://img.shields.io/badge/Python-3.8%2B-blue?style=for-the-badge&logo=python)
![Streamlit](https://img.shields.io/badge/Streamlit-1.35%2B-red?style=for-the-badge&logo=streamlit)

**🚀 基于AI的智能数据分析平台 - 支持自然语言查询、多格式文件处理和可视化分析**

[功能特性](#-功能特性) • [快速开始](#-快速开始) • [界面展示](#-界面展示) • [部署指南](#-部署指南) • [技术栈](#-技术栈)

</div>

---

## 🌟 功能特性

### 📁 多格式文件支持
- **表格文件**: CSV, Excel (.xls, .xlsx)
- **图片文件**: JPG, PNG (OCR识别)
- **文档文件**: PDF (OCR识别)
- **智能编码检测**: 自动识别文件编码格式

### 🤖 AI智能分析
- **自然语言查询**: 使用XiYan-SQL模型将自然语言转换为SQL
- **OCR表格提取**: 基于Qwen-VL模型的图片/PDF表格识别
- **智能数据清洗**: 自动处理缺失值、格式标准化

### 📊 数据可视化
- **交互式图表**: 基于Plotly的动态图表生成
- **多种图表类型**: 柱状图、折线图、饼图等
- **结果导出**: 支持CSV格式下载

### 🔧 企业级特性
- **数据库集成**: PostgreSQL数据库自动管理
- **会话管理**: 多会话支持，数据隔离
- **连接池**: 高性能数据库连接管理
- **错误处理**: 统一的错误处理和日志记录

---

## 🚀 快速开始

### 环境要求

- **Python**: 3.8+
- **数据库**: PostgreSQL 12+
- **内存**: 推荐4GB+
- **存储**: 推荐1GB+

### 一键安装

```bash
# 克隆项目
git clone https://github.com/your-username/xiyan-data-analysis-assistant.git
cd xiyan-data-analysis-assistant

# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/macOS
# 或 venv\Scripts\activate  # Windows

# 安装依赖
pip install -r requirements.txt

# 配置环境变量
cp .env.example .env
# 编辑 .env 文件，填入你的配置信息

# 启动应用
streamlit run app.py
```

### Docker部署

```bash
# 克隆项目
git clone https://github.com/your-username/xiyan-data-analysis-assistant.git
cd xiyan-data-analysis-assistant/docker

# 配置环境变量
cp .env.example .env
# 编辑 .env 文件

# 启动服务
docker-compose up --build -d
```

访问 `http://localhost:8501` 即可使用

---

## 🎨 界面展示

### 📊 数据库连接
![数据库连接界面](./screenshots/0.数据库连接.png)

### 📁 文件上传
![文件上传界面](./screenshots/1.上传文件数据.png)

### 💬 智能分析
![提问与分析界面](./screenshots/2.提问与分析.png)

### 📈 可视化结果
![查询结果与图表分析](./screenshots/3.查询结果与图表分析.png)

---

## ⚙️ 部署指南

### 环境配置

创建 `.env` 文件并配置以下参数：

```env
# 视觉语言模型配置 (OCR功能)
VL_MODEL_BASEURL=YOUR_VL_MODEL_API_BASE_URL
VL_MODEL_KEY=YOUR_VL_MODEL_API_KEY
VL_MODEL_NAME=qwen-vl-plus

# SQL生成模型配置
SQL_MODEL_BASEURL=YOUR_SQL_MODEL_API_BASE_URL
SQL_MODEL_KEY=YOUR_SQL_MODEL_API_KEY
SQL_MODEL_NAME=sql-agent

# 数据库连接配置
DB_HOST=localhost
DB_PORT=5432
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_DATABASE=your_db_name
```

### 推荐模型

#### XiYanSQL-QwenCoder-32B-2412
专为SQL生成优化的模型
- 🤗 [HuggingFace](https://huggingface.co/XGenerationLab/XiYanSQL-QwenCoder-32B-2412)
- 🎯 [魔搭](https://modelscope.cn/models/XGenerationLab/XiYanSQL-QwenCoder-32B-2412)

#### Qwen2.5-VL-7B-Instruct
强大的视觉语言模型
- 🤗 [HuggingFace](https://huggingface.co/Qwen/Qwen2.5-VL-7B-Instruct)
- 🎯 [魔搭](https://modelscope.cn/models/Qwen/Qwen2.5-VL-7B-Instruct)

---

## 🛠️ 技术栈

### 前端技术
- **Streamlit**: 现代化Web应用框架
- **Plotly**: 交互式数据可视化
- **HTML/CSS**: 界面美化

### 后端技术
- **Python**: 核心开发语言
- **Pandas**: 数据处理和分析
- **NumPy**: 数值计算
- **PostgreSQL**: 主数据库

### AI模型
- **XiYan-SQL**: 自然语言转SQL
- **Qwen-VL**: 视觉语言模型
- **OpenAI API**: 通用API接口

### 文件处理
- **PyMuPDF**: PDF文件处理
- **python-calamine**: Excel文件解析
- **chardet**: 文件编码检测
- **Pillow**: 图像处理

---

## 📋 使用说明

### 基本使用流程

1. **启动应用**: 运行 `streamlit run app.py`
2. **连接数据库**: 在侧边栏配置数据库连接
3. **上传文件**: 拖拽或点击上传数据文件
4. **自然语言查询**: 输入问题如"统计每个产品的销售总额"
5. **查看结果**: 获得表格和图表分析结果
6. **导出数据**: 下载CSV格式的分析结果

### 支持的查询类型

- **统计分析**: "计算平均值、总和、最大值等"
- **数据筛选**: "显示销售额大于1000的产品"
- **数据排序**: "按价格从高到低排序"
- **数据分组**: "按地区统计销售额"
- **数据删除**: "删除临时表数据"

---

## 🤝 贡献指南

我们欢迎所有形式的贡献！

### 开发环境设置

```bash
# 克隆项目
git clone https://github.com/your-username/xiyan-data-analysis-assistant.git
cd xiyan-data-analysis-assistant

# 创建开发分支
git checkout -b feature/your-feature-name

# 安装开发依赖
pip install -r requirements.txt
pip install pytest black flake8

# 运行测试
pytest tests/

# 代码格式化
black .
flake8 .
```

### 提交规范

- **功能**: `feat: 添加新功能`
- **修复**: `fix: 修复bug`
- **文档**: `docs: 更新文档`
- **样式**: `style: 代码格式化`
- **重构**: `refactor: 代码重构`
- **测试**: `test: 添加测试`

---

## 📝 更新日志

### v0.3.1 (2025-08-03)

#### 🎉 重大优化
- **文件解析稳定性**: 增强CSV、Excel、PDF解析的兼容性
- **UI界面美化**: 全新的现代化界面设计
- **性能优化**: 添加数据库连接池和批量插入优化
- **错误处理**: 统一的错误处理机制

#### 🔧 技术改进
- **CSV解析**: 多种编码尝试机制 (utf-8, gbk, gb2312, latin1)
- **Excel解析**: 多引擎支持 (calamine, openpyxl, xlrd)
- **OCR优化**: 内存管理和图片格式转换优化
- **数据库**: 连接池支持和事务处理优化

#### 🎨 界面优化
- **布局优化**: 响应式列布局设计
- **文件上传**: 美化的拖拽上传界面
- **数据展示**: 改进的表格和图表显示
- **用户体验**: 统一的图标和色彩主题

#### 🤖 AI优化
- **Claude Code**: 由Claude Code协助完成代码优化和重构
- **代码质量**: 遵循KISS原则，提升代码可维护性
- **错误处理**: 标准化的错误消息和用户建议

---

## 📄 许可证

本项目采用 [MIT 许可证](LICENSE) - 查看 [LICENSE](LICENSE) 文件了解详情。

---

## 🙏 致谢

### 特别感谢
- **Claude Code** - 感谢Claude Code在v0.3.1版本优化中提供的代码重构、性能优化和界面美化支持
- **XiYanSQL团队** - 提供优秀的SQL生成模型
- **Qwen团队** - 提供强大的视觉语言模型

### 技术支持
- [Streamlit](https://streamlit.io/) - 现代化Web应用框架
- [Pandas](https://pandas.pydata.org/) - 数据处理库
- [Plotly](https://plotly.com/) - 数据可视化库
- [PostgreSQL](https://www.postgresql.org/) - 关系型数据库

---

## 📞 联系我们

- **问题反馈**: [GitHub Issues](https://github.com/your-username/xiyan-data-analysis-assistant/issues)
- **功能建议**: [GitHub Discussions](https://github.com/your-username/xiyan-data-analysis-assistant/discussions)
- **邮件联系**: your-email@example.com

---

<div align="center">

**⭐ 如果这个项目对您有帮助，请给我们一个Star！**

![Star History](https://img.shields.io/github/stars/your-username/xiyan-data-analysis-assistant?style=social)

</div>