# 多语言 GitHub Pull Request 深度分析爬虫 (Multi-Language GitHub PR Deep Analysis Crawler)

这是一个功能强大的 Python 脚本，用于从 GitHub 上爬取指定编程语言的热门仓库中**已合并 (Merged)** 的 **Pull Requests (PR)**，并对其进行深度的、基于语义的分析。

该脚本不仅能抓取 PR 的元数据（如标题、描述、评论），还能深入到每一次 **Commit** 的代码变更中，利用正则表达式解析 **diff/patch** 内容，提取出函数、类、依赖库的变更信息。所有数据都以结构化的 **JSONL** 格式保存，便于后续进行数据分析、模型训练或学术研究。

-----

## 🚀 核心功能 (Core Features)

  * **多语言支持**: 可配置爬取多种主流编程语言的仓库。
  * **深度语义分析**:
      * **函数变更检测**: 识别在 PR 中被添加、删除或修改的函数。
      * **类/结构体变更检测**: 识别在 PR 中被添加、删除或修改的类或结构体。
      * **依赖提取**: 解析代码文件，提取其导入的依赖库/模块。
      * **Diff Hunk 解析**: 将 `patch` 内容结构化，方便分析具体的代码行变更。
  * **全面的数据采集**: 获取包括 PR 元数据、Commits、文件变更、审查评论 (Reviews & Comments) 在内的完整信息。
  * **强大的容错与续爬机制**:
      * 自动处理 GitHub API 速率限制 (Rate Limit) 和网络错误。
      * 能够跳过已处理的仓库，支持从上次中断的地方继续爬取。
  * **结构化输出**: 所有数据均以 `.jsonl` 格式保存，每行一个 JSON 对象，方便处理。

-----

## ⚙️ 支持的语言 (Supported Languages)

脚本通过 `MultiLanguageSemanticAnalyzer` 类中的 `LANGUAGE_CONFIG` 来支持不同语言的语义分析。在 `main` 函数中的 `languages` 列表中，您可以填入以下一个或多个语言名称的字符串：

  * `'python'`
  * `'javascript'`
  * `'java'`
  * `'golang'`
  * `'cpp'`
  * `'typescript'`

**注意**:

  * 对于 C++，脚本会识别 `.cpp`, `.cc`, `.cxx`, `.h`, `.hpp` 等多种扩展名。
  * 对于 Go，脚本会正确识别 `go` 语言。
  * 对于 JavaScript 和 TypeScript，脚本会识别 `.js`, `.jsx`, `.ts`, `.tsx` 等扩展名。

-----

## 🔧 安装与配置 (Installation & Configuration)

在运行脚本之前，请按照以下步骤进行配置：

**1. 安装依赖库**

该脚本主要依赖 `requests` 库。您可以通过 pip 安装：

```bash
pip install requests
```

**2. 获取 GitHub Personal Access Token**

为了访问 GitHub API 并获得更高的速率限制，您需要一个 Personal Access Token (PAT)。

  * 访问 GitHub [**设置 -\> Developer settings -\> Personal access tokens**](https://github.com/settings/tokens) 来创建一个新的 Token。
  * 确保授予 `repo` 和 `read:org` 权限，以便能够访问公共仓库的详细信息。

**3. 创建 `.env` 文件**

在项目根目录下创建一个名为 `.env` 的文件。该文件用于存放您的 GitHub Token，以避免硬编码在代码中。文件内容如下：

```
GITHUB_TOKEN=ghp_YourPersonalAccessTokenHere
```

请将 `ghp_YourPersonalAccessTokenHere` 替换为您自己生成的 Token。

**4. 准备仓库列表文件**

脚本需要从本地文件中读取要爬取的仓库列表。

  * 请确保项目根目录下存在一个名为 `top_2000_star_repos_this_year` 的文件夹。
  * 在该文件夹中，存放以 `{language}_stars_this_year.jsonl` 格式命名的文件，例如 `top_javascript_stars_this_year.jsonl`。
  * 这些文件应该是 JSONL 格式，每行包含一个仓库信息的 JSON 对象，至少需要包含 `repo_name` (如 `facebook/react`) 和 `star_count` 字段。

-----

## ▶️ 如何运行 (How to Run)

1.  **配置爬取目标**:
    打开 `crawler.py` 文件，找到文件底部的 `main` 函数。

      * 在 `languages` 列表中设置您想要爬取的语言。
        ```python
        # 示例：同时爬取 JavaScript 和 Python 的数据
        languages = [
            'javascript',
            'python'
        ]
        ```
      * 您可以设置 `target_repos_per_language` 来指定每种语言要成功处理的仓库数量。

2.  **执行脚本**:
    在您的终端中，导航到项目根目录并运行脚本：

    ```bash
    python crawler.py
    ```

脚本会开始运行，并在终端中打印详细的日志信息，包括当前处理的仓库、PR 进度以及 API 速率限制等。

-----

## 📊 输出数据结构 (Output Data Structure)

所有爬取和分析的数据都将保存在 `github_pr_data` 文件夹中，并按语言和数据类型分类命名。

  * `{language}_pr_data.jsonl`: PR 的核心元数据，包括标题、作者、统计信息（代码增删、文件变更数等）。
  * `{language}_commits.jsonl`: 每个 PR 包含的所有 commit 的详细信息。
  * `{language}_file_changes.jsonl`: 每次 commit 中涉及的文件变更列表，包含 `patch` 内容。
  * `{language}_review_comments.jsonl`: PR 页面上的所有审查评论和代码行评论。
  * `{language}_function_changes.jsonl`: 从 `patch` 中提取到的**函数变更**的详细信息。
  * `{language}_class_changes.jsonl`: 从 `patch` 中提取到的**类/结构体变更**的详细信息。
  * `{language}_imports.jsonl`: 从变更的文件中提取到的**依赖导入**语句。
  * `{language}_diff_hunks.jsonl`: `patch` 内容被结构化解析后的数据块 (Hunks)。
