# GitHub 推送配置指南

本指南将帮助您配置 GitHub 认证，以便能够推送代码到远程仓库。

## 方法一：配置 SSH 密钥（推荐）

### 步骤 1：检查 SSH 密钥

您的 SSH 公钥已存在：
```
ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIO9kF+AtjGbVr1atMjpDFec1rstbu7Nb92lr3Ms10ROk wuyang.zhang
```

### 步骤 2：将 SSH 公钥添加到 GitHub

1. **复制您的 SSH 公钥**（已在上面显示）

2. **登录 GitHub**，访问：https://github.com/settings/keys

3. **点击 "New SSH key"** 按钮

4. **填写信息**：
   - **Title**: 给这个密钥起个名字（如：MacBook Pro）
   - **Key**: 粘贴上面的完整 SSH 公钥
   - **Key type**: 选择 "Authentication Key"

5. **点击 "Add SSH key"** 保存

### 步骤 3：测试 SSH 连接

在终端运行：
```bash
ssh -T git@github.com
```

如果看到类似以下消息，说明配置成功：
```
Hi EthanJ-Sss! You've successfully authenticated, but GitHub does not provide shell access.
```

### 步骤 4：推送代码

配置成功后，运行：
```bash
git push origin main
```

---

## 方法二：使用 Personal Access Token（备选方案）

如果 SSH 配置遇到问题，可以使用 Personal Access Token。

### 步骤 1：创建 Personal Access Token

1. **登录 GitHub**，访问：https://github.com/settings/tokens

2. **点击 "Generate new token"** → **"Generate new token (classic)"**

3. **填写信息**：
   - **Note**: 给 token 起个名字（如：fund-project）
   - **Expiration**: 选择过期时间（建议 90 天或自定义）
   - **Select scopes**: 勾选 `repo`（完整仓库访问权限）

4. **点击 "Generate token"**

5. **重要**：立即复制生成的 token（类似：`ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`），**只显示一次**！

### 步骤 2：配置 Git 使用 Token

有两种方式：

#### 方式 A：使用 Git Credential Helper（推荐）

```bash
# 配置 credential helper
git config --global credential.helper osxkeychain

# 推送时输入用户名和 token
git push origin main
# Username: EthanJ-Sss
# Password: <粘贴您的 token>
```

#### 方式 B：在 URL 中直接使用 Token

```bash
# 将远程 URL 改为包含 token 的格式
git remote set-url origin https://<YOUR_TOKEN>@github.com/EthanJ-Sss/fund.git

# 然后推送
git push origin main
```

**注意**：方式 B 会将 token 存储在 `.git/config` 中，安全性较低，不推荐。

---

## 当前状态

- ✅ 远程仓库已配置为 SSH 方式：`git@github.com:EthanJ-Sss/fund.git`
- ✅ SSH 密钥已加载到 SSH agent
- ⚠️ SSH 公钥需要添加到 GitHub 账户

## 推荐操作

**优先使用方法一（SSH）**：
1. 将 SSH 公钥添加到 GitHub（步骤 2）
2. 测试连接：`ssh -T git@github.com`
3. 推送代码：`git push origin main`

如果 SSH 配置有问题，再使用方法二（Personal Access Token）。

---

## 常见问题

### Q: SSH 连接仍然失败？
A: 检查以下几点：
- 确认公钥已正确添加到 GitHub
- 确认 SSH agent 中有密钥：`ssh-add -l`
- 检查 SSH 配置：`cat ~/.ssh/config`

### Q: Token 在哪里查看？
A: 访问 https://github.com/settings/tokens 可以查看和管理所有 token。

### Q: 如何撤销 Token？
A: 在 https://github.com/settings/tokens 页面，找到对应的 token，点击 "Revoke" 即可。

### Q: 忘记 Token 了怎么办？
A: Token 只显示一次，如果忘记了需要重新创建。

---

## 快速命令参考

```bash
# 查看远程仓库配置
git remote -v

# 测试 SSH 连接
ssh -T git@github.com

# 查看 SSH 密钥
cat ~/.ssh/id_ed25519.pub

# 添加 SSH 密钥到 agent
ssh-add ~/.ssh/id_ed25519

# 推送代码
git push origin main
```
