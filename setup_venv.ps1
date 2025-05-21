# 创建虚拟环境
python -m venv venv

# 激活虚拟环境
.\venv\Scripts\Activate.ps1

# 升级pip
python -m pip install --upgrade pip

# 安装依赖
pip install -r requirements.txt

Write-Host "虚拟环境设置完成！"
Write-Host "使用 '.\venv\Scripts\Activate.ps1' 来激活虚拟环境" 