import requests
import json
import time
import os
import re
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MultiLanguageSemanticAnalyzer:
    """多语言代码语义分析器"""
    
    # 语言配置
    LANGUAGE_CONFIG = {
        'python': {
            'file_extensions': ['.py'],
            'import_patterns': [
                r'^\s*import\s+(.+)',
                r'^\s*from\s+([^\s]+)\s+import\s+(.+)'
            ],
            'function_patterns': [
                r'def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\('
            ],
            'class_patterns': [
                r'class\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*[\(:]'
            ],
            'comment_patterns': [r'#.*$', r'""".*?"""', r"'''.*?'''"]
        },
        'javascript': {
            'file_extensions': ['.js', '.jsx', '.ts', '.tsx'],
            'import_patterns': [
                r'^\s*import\s+(.+?)\s+from\s+["\']([^"\']+)["\']',
                r'^\s*import\s+["\']([^"\']+)["\']',
                r'^\s*const\s+(.+?)\s*=\s*require\s*\(\s*["\']([^"\']+)["\']\s*\)',
                r'^\s*require\s*\(\s*["\']([^"\']+)["\']\s*\)'
            ],
            'function_patterns': [
                r'function\s+([a-zA-Z_$][a-zA-Z0-9_$]*)\s*\(',
                r'const\s+([a-zA-Z_$][a-zA-Z0-9_$]*)\s*=\s*\(',
                r'([a-zA-Z_$][a-zA-Z0-9_$]*)\s*:\s*function\s*\(',
                r'([a-zA-Z_$][a-zA-Z0-9_$]*)\s*\([^)]*\)\s*=>'
            ],
            'class_patterns': [
                r'class\s+([a-zA-Z_$][a-zA-Z0-9_$]*)\s*[{(]'
            ],
            'comment_patterns': [r'//.*$', r'/\*.*?\*/']
        },
        'java': {
            'file_extensions': ['.java'],
            'import_patterns': [
                r'^\s*import\s+(?:static\s+)?([^;]+);'
            ],
            'function_patterns': [
                r'(?:public|private|protected|static|\s)*\s+\w+\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\('
            ],
            'class_patterns': [
                r'(?:public|private|protected|abstract|final|\s)*\s*class\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*[{<]'
            ],
            'comment_patterns': [r'//.*$', r'/\*.*?\*/']
        },
        'golang': {
            'file_extensions': ['.go'],
            'import_patterns': [
                r'^\s*import\s+"([^"]+)"',
                r'^\s*import\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+"([^"]+)"',
                r'^\s*import\s+\(\s*\n((?:\s*"[^"]+"\s*\n)*)\s*\)'
            ],
            'function_patterns': [
                r'func\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(',
                r'func\s+\([^)]*\)\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\('
            ],
            'class_patterns': [
                r'type\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+struct\s*{'
            ],
            'comment_patterns': [r'//.*$', r'/\*.*?\*/']
        },
        'cpp': {
            'file_extensions': ['.cpp', '.cc', '.cxx', '.c++', '.hpp', '.h'],
            'import_patterns': [
                r'^\s*#include\s*<([^>]+)>',
                r'^\s*#include\s*"([^"]+)"',
                r'^\s*using\s+namespace\s+([^;]+);'
            ],
            'function_patterns': [
                r'(?:inline\s+)?(?:static\s+)?(?:virtual\s+)?(?:const\s+)?\w+\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(',
                r'([a-zA-Z_][a-zA-Z0-9_]*)::[a-zA-Z_][a-zA-Z0-9_]*\s*\('
            ],
            'class_patterns': [
                r'class\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*[{:]',
                r'struct\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*[{:]'
            ],
            'comment_patterns': [r'//.*$', r'/\*.*?\*/']
        },
        'typescript': {
            'file_extensions': ['.ts', '.tsx'],
            'import_patterns': [
                r'^\s*import\s+(.+?)\s+from\s+["\']([^"\']+)["\']',
                r'^\s*import\s+["\']([^"\']+)["\']',
                r'^\s*import\s+type\s+(.+?)\s+from\s+["\']([^"\']+)["\']'
            ],
            'function_patterns': [
                r'function\s+([a-zA-Z_$][a-zA-Z0-9_$]*)\s*\(',
                r'const\s+([a-zA-Z_$][a-zA-Z0-9_$]*)\s*=\s*\(',
                r'([a-zA-Z_$][a-zA-Z0-9_$]*)\s*:\s*function\s*\(',
                r'([a-zA-Z_$][a-zA-Z0-9_$]*)\s*\([^)]*\)\s*=>'
            ],
            'class_patterns': [
                r'class\s+([a-zA-Z_$][a-zA-Z0-9_$]*)\s*[{<]',
                r'interface\s+([a-zA-Z_$][a-zA-Z0-9_$]*)\s*[{<]'
            ],
            'comment_patterns': [r'//.*$', r'/\*.*?\*/']
        }
    }
    
    @staticmethod
    def get_language_from_file(file_path: str) -> Optional[str]:
        """根据文件扩展名判断编程语言"""
        ext = Path(file_path).suffix.lower()
        
        for language, config in MultiLanguageSemanticAnalyzer.LANGUAGE_CONFIG.items():
            if ext in config['file_extensions']:
                return language
        return None
    
    @staticmethod
    def extract_imports(file_content: str, language: str) -> List[Dict[str, Any]]:
        """提取指定语言的导入语句"""
        if language not in MultiLanguageSemanticAnalyzer.LANGUAGE_CONFIG:
            return []
        
        config = MultiLanguageSemanticAnalyzer.LANGUAGE_CONFIG[language]
        imports = []
        lines = file_content.split('\n')
        
        for line_num, line in enumerate(lines, 1):
            stripped = line.strip()
            
            # 跳过空行和注释
            if not stripped:
                continue
            
            # 检查是否是注释
            is_comment = False
            for comment_pattern in config['comment_patterns']:
                if re.search(comment_pattern, stripped):
                    is_comment = True
                    break
            
            if is_comment:
                continue
            
            # 匹配导入语句
            for pattern in config['import_patterns']:
                match = re.search(pattern, stripped)
                if match:
                    import_info = {
                        'language': language,
                        'line_number': line_num,
                        'import_statement': stripped,
                        'match_groups': match.groups()
                    }
                    
                    # 根据语言特定处理
                    if language == 'python':
                        if 'from' in stripped:
                            import_info['import_type'] = 'from_import'
                            import_info['module_name'] = match.group(1)
                            import_info['imported_items'] = [item.strip() for item in match.group(2).split(',')]
                        else:
                            import_info['import_type'] = 'import'
                            import_info['module_name'] = match.group(1)
                            import_info['imported_items'] = None
                    
                    elif language in ['javascript', 'typescript']:
                        if 'from' in stripped:
                            import_info['import_type'] = 'es6_import'
                            import_info['imported_items'] = match.group(1).strip()
                            import_info['module_name'] = match.group(2)
                        elif 'require' in stripped:
                            import_info['import_type'] = 'require'
                            import_info['module_name'] = match.group(1) if len(match.groups()) == 1 else match.group(2)
                    
                    elif language == 'java':
                        import_info['import_type'] = 'import'
                        import_info['module_name'] = match.group(1)
                    
                    elif language == 'golang':
                        import_info['import_type'] = 'import'
                        import_info['module_name'] = match.group(1)
                    
                    elif language == 'cpp':
                        if '<' in stripped:
                            import_info['import_type'] = 'system_include'
                            import_info['module_name'] = match.group(1)
                        elif '"' in stripped:
                            import_info['import_type'] = 'local_include'
                            import_info['module_name'] = match.group(1)
                        elif 'using' in stripped:
                            import_info['import_type'] = 'using_namespace'
                            import_info['module_name'] = match.group(1)
                    
                    imports.append(import_info)
                    break
        
        return imports
    
    @staticmethod
    def detect_function_changes(patch_content: str, language: str) -> List[Dict[str, Any]]:
        """检测函数变更（多语言支持）"""
        if language not in MultiLanguageSemanticAnalyzer.LANGUAGE_CONFIG:
            return []
        
        config = MultiLanguageSemanticAnalyzer.LANGUAGE_CONFIG[language]
        functions = []
        hunks = MultiLanguageSemanticAnalyzer.parse_diff_hunks(patch_content)
        
        for hunk in hunks:
            # 检查hunk的context信息
            context = hunk.get('context', '')
            for pattern in config['function_patterns']:
                match = re.search(pattern, context)
                if match:
                    functions.append({
                        'function_name': match.group(1),
                        'change_type': 'modified',
                        'line_content': context.strip(),
                        'source': 'context',
                        'language': language
                    })
            
            # 检查变更内容中的函数定义
            for change_line in hunk['changes']:
                if change_line.startswith(('+', '-')):
                    for pattern in config['function_patterns']:
                        match = re.search(pattern, change_line)
                        if match:
                            change_type = 'added' if change_line.startswith('+') else 'removed'
                            functions.append({
                                'function_name': match.group(1),
                                'change_type': change_type,
                                'line_content': change_line[1:].strip(),
                                'source': 'diff_content',
                                'language': language
                            })
        
        # 去重
        unique_functions = {}
        for func in functions:
            key = f"{func['function_name']}_{func['language']}"
            if key not in unique_functions:
                unique_functions[key] = func
            else:
                if func['source'] == 'diff_content':
                    unique_functions[key] = func
        
        return list(unique_functions.values())
    
    @staticmethod
    def detect_class_changes(patch_content: str, language: str) -> List[Dict[str, Any]]:
        """检测类变更（多语言支持）"""
        if language not in MultiLanguageSemanticAnalyzer.LANGUAGE_CONFIG:
            return []
        
        config = MultiLanguageSemanticAnalyzer.LANGUAGE_CONFIG[language]
        classes = []
        hunks = MultiLanguageSemanticAnalyzer.parse_diff_hunks(patch_content)
        
        for hunk in hunks:
            # 检查hunk的context信息
            context = hunk.get('context', '')
            for pattern in config['class_patterns']:
                match = re.search(pattern, context)
                if match:
                    classes.append({
                        'class_name': match.group(1),
                        'change_type': 'modified',
                        'line_content': context.strip(),
                        'source': 'context',
                        'language': language
                    })
            
            # 检查变更内容中的类定义
            for change_line in hunk['changes']:
                if change_line.startswith(('+', '-')):
                    for pattern in config['class_patterns']:
                        match = re.search(pattern, change_line)
                        if match:
                            change_type = 'added' if change_line.startswith('+') else 'removed'
                            classes.append({
                                'class_name': match.group(1),
                                'change_type': change_type,
                                'line_content': change_line[1:].strip(),
                                'source': 'diff_content',
                                'language': language
                            })
        
        # 去重
        unique_classes = {}
        for cls in classes:
            key = f"{cls['class_name']}_{cls['language']}"
            if key not in unique_classes:
                unique_classes[key] = cls
            else:
                if cls['source'] == 'diff_content':
                    unique_classes[key] = cls
        
        return list(unique_classes.values())
    
    @staticmethod
    def parse_diff_hunks(patch_content: str) -> List[Dict[str, Any]]:
        """解析diff内容为hunks"""
        hunks = []
        current_hunk = None
        
        for line in patch_content.split('\n'):
            if line.startswith('@@'):
                match = re.match(r'@@ -(\d+),?(\d+)? \+(\d+),?(\d+)? @@(.+)?', line)
                if match:
                    old_start = int(match.group(1))
                    old_count = int(match.group(2) or 1)
                    new_start = int(match.group(3))
                    new_count = int(match.group(4) or 1)
                    context = match.group(5) or ''
                    
                    current_hunk = {
                        'old_start': old_start,
                        'old_count': old_count,
                        'new_start': new_start,
                        'new_count': new_count,
                        'context': context.strip(),
                        'changes': []
                    }
                    hunks.append(current_hunk)
            
            elif current_hunk and line.startswith(('+', '-', ' ')):
                current_hunk['changes'].append(line)
        
        return hunks

class MultiLanguageGitHubPRCrawler:
    def __init__(self, token: str, repos_dir: str = "top_2000_star_repos_this_year", 
                 output_dir: str = "github_pr_data"):
        """
        初始化多语言 GitHub PR 爬虫
        
        Args:
            token: GitHub Personal Access Token
            repos_dir: 存放仓库列表JSONL文件的目录
            output_dir: 输出数据的目录
        """
        self.token = token
        self.headers = {
            'Authorization': f'token {token}',
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'GitHub-PR-Crawler'
        }
        self.base_url = 'https://api.github.com'
        self.repos_dir = Path(repos_dir)
        self.output_dir = Path(output_dir)
        
        # 创建输出目录
        self.output_dir.mkdir(exist_ok=True)
        
        # 初始化多语言语义分析器
        self.analyzer = MultiLanguageSemanticAnalyzer()
        
        # API 调用计数和限制
        self.api_calls = 0
        self.rate_limit_remaining = 5000
        
        # 统计信息
        self.stats = {
            'total_repos_attempted': 0,
            'repos_successfully_processed': 0,
            'repos_skipped_no_prs': 0,
            'repos_skipped_too_many_prs': 0,
            'repos_failed': 0,
            'total_prs_processed': 0,
            'repos_skipped_already_processed': 0,
            'functions_detected': 0,
            'classes_detected': 0,
            'imports_extracted': 0,
            'language_stats': {}
        }
    
    def check_rate_limit(self) -> None:
        """检查并处理 API 速率限制"""
        if self.rate_limit_remaining < 50:
            logger.warning(f"API rate limit low: {self.rate_limit_remaining}, waiting...")
            time.sleep(60)
    
    def make_request(self, url: str, params: Dict = None, max_retries: int = 3) -> Optional[Dict]:
        """发送 API 请求并处理响应"""
        for attempt in range(max_retries):
            self.check_rate_limit()
            
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                self.api_calls += 1
                
                self.rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                rate_limit_reset = response.headers.get('X-RateLimit-Reset', 0)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 403:
                    logger.error("Rate limit exceeded, waiting...")
                    reset_time = int(rate_limit_reset) - int(time.time())
                    wait_time = max(reset_time, 3600)
                    time.sleep(wait_time)
                    return self.make_request(url, params, max_retries=1)
                elif response.status_code == 404:
                    logger.warning(f"Resource not found: {url}")
                    return None
                elif response.status_code in [502, 503, 504]:
                    logger.warning(f"Server error {response.status_code}, retrying in {2 ** attempt} seconds...")
                    time.sleep(2 ** attempt)
                    continue
                else:
                    logger.error(f"API request failed: {response.status_code} - {response.text}")
                    return None
                    
            except Exception as e:
                logger.error(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                else:
                    return None
    
    def load_repos_from_file(self, language: str, min_stars: int = 1000, 
                           start_index: int = 0, batch_size: int = 50) -> List[Dict]:
        """从JSONL文件中加载指定语言的仓库列表"""
        # 语言映射
        language_mapping = {
            'cpp': 'c++',
            'golang': 'go'
        }
        
        file_language = language_mapping.get(language, language)
        file_path = self.repos_dir / f"top_{file_language}_stars_this_year.jsonl"
        
        if not file_path.exists():
            logger.error(f"Repository file not found: {file_path}")
            return []
        
        logger.info(f"Loading repositories from: {file_path} (starting from index {start_index}, batch size {batch_size})")
        
        repos = []
        current_index = 0
        loaded_count = 0
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        repo_data = json.loads(line.strip())
                        
                        if repo_data.get('star_count', 0) >= min_stars:
                            if current_index >= start_index:
                                repo_name = repo_data['repo_name']
                                if '/' in repo_name:
                                    owner, name = repo_name.split('/', 1)
                                    
                                    repo_info = {
                                        'owner': {'login': owner},
                                        'name': name,
                                        'full_name': repo_name,
                                        'language': repo_data.get('language'),
                                        'stargazers_count': repo_data.get('star_count', 0),
                                        'html_url': f"https://github.com/{repo_name}",
                                        'pushed_at': repo_data.get('latest_pushed_time')
                                    }
                                    repos.append(repo_info)
                                    loaded_count += 1
                                    
                                    if loaded_count >= batch_size:
                                        break
                            
                            current_index += 1
            
            logger.info(f"Loaded {len(repos)} repositories for {language}")
            return repos
            
        except Exception as e:
            logger.error(f"Error loading repositories from {file_path}: {e}")
            return []
    
    def save_to_jsonl(self, data: Dict, file_path: Path):
        """保存数据到JSONL文件"""
        try:
            with open(file_path, 'a', encoding='utf-8') as f:
                f.write(json.dumps(data, ensure_ascii=False) + '\n')
        except Exception as e:
            logger.error(f"Error saving to {file_path}: {e}")
    
    def get_processed_repos_set(self, language: str) -> set:
        """获取已处理的仓库集合"""
        processed_repos = set()
        
        # 检查各种输出文件
        output_files = [
            self.output_dir / f"{language}_pr_data.jsonl",
            self.output_dir / f"{language}_commits.jsonl",
            self.output_dir / f"{language}_file_changes.jsonl",
            self.output_dir / f"{language}_function_changes.jsonl",
            self.output_dir / f"{language}_class_changes.jsonl",
            self.output_dir / f"{language}_imports.jsonl"
        ]
        
        for file_path in output_files:
            if file_path.exists():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            if line.strip():
                                data = json.loads(line.strip())
                                repo_full_name = data.get('repo_full_name')
                                if repo_full_name:
                                    processed_repos.add(repo_full_name)
                except Exception as e:
                    logger.warning(f"Error reading {file_path}: {e}")
        
        logger.info(f"Found {len(processed_repos)} already processed repos")
        return processed_repos
    
    def get_file_content_at_commit(self, owner: str, repo_name: str, file_path: str, commit_sha: str) -> Optional[str]:
        """获取文件在特定commit时间点的内容"""
        try:
            url = f"{self.base_url}/repos/{owner}/{repo_name}/contents/{file_path}"
            params = {'ref': commit_sha}
            
            data = self.make_request(url, params)
            if not data:
                return None
            
            # GitHub API返回base64编码的内容
            if data.get('encoding') == 'base64':
                import base64
                content = base64.b64decode(data['content']).decode('utf-8', errors='ignore')
                return content
            
        except Exception as e:
            logger.warning(f"Error getting file content at commit {commit_sha[:8]}: {e}")
        return None
    
    def get_pull_requests(self, owner: str, repo: str, state: str = 'closed', 
                         max_prs: int = None, skip_if_too_many: int = 100) -> List[Dict]:
        """获取仓库的所有 Pull Requests"""
        logger.info(f"Getting all PRs for {owner}/{repo}")
        
        prs = []
        page = 1
        
        while True:
            url = f"{self.base_url}/repos/{owner}/{repo}/pulls"
            params = {
                'state': state,
                'sort': 'updated',
                'direction': 'desc',
                'per_page': 100,
                'page': page
            }
            
            data = self.make_request(url, params)
            if not data:
                break
            
            merged_prs = [pr for pr in data if pr.get('merged_at')]
            prs.extend(merged_prs)
            
            logger.info(f"Found {len(merged_prs)} merged PRs on page {page}, total: {len(prs)}")
            
            # 检查是否需要跳过
            if page == 1 and len(merged_prs) > 80 and len(data) == 100:
                estimated_total = len(merged_prs) * 10
                if estimated_total > skip_if_too_many:
                    logger.warning(f"Skipping {owner}/{repo} - estimated {estimated_total} merged PRs")
                    self.stats['repos_skipped_too_many_prs'] += 1
                    return []
            
            if len(prs) > skip_if_too_many:
                logger.warning(f"Skipping {owner}/{repo} - found {len(prs)} merged PRs")
                self.stats['repos_skipped_too_many_prs'] += 1
                return []
            
            if len(data) < 100:
                break
            
            if max_prs and len(prs) >= max_prs:
                prs = prs[:max_prs]
                break
            
            page += 1
            time.sleep(0.5)
        
        logger.info(f"Found total {len(prs)} merged PRs for {owner}/{repo}")
        return prs
    
    def get_pr_commits(self, owner: str, repo: str, pr_number: int) -> List[Dict]:
        """获取 PR 的所有 commits"""
        commits = []
        page = 1
        
        while True:
            url = f"{self.base_url}/repos/{owner}/{repo}/pulls/{pr_number}/commits"
            params = {'per_page': 100, 'page': page}
            
            data = self.make_request(url, params)
            if not data:
                break
            
            commits.extend(data)
            
            if len(data) < 100:
                break
            
            page += 1
            time.sleep(0.2)
        
        return commits
    
    def get_pr_files(self, owner: str, repo: str, pr_number: int) -> List[Dict]:
        """获取 PR 修改的文件"""
        files = []
        page = 1
        
        while True:
            url = f"{self.base_url}/repos/{owner}/{repo}/pulls/{pr_number}/files"
            params = {'per_page': 100, 'page': page}
            
            data = self.make_request(url, params)
            if not data:
                break
            
            files.extend(data)
            
            if len(data) < 100:
                break
            
            page += 1
            time.sleep(0.2)
        
        return files
    
    def get_pr_reviews(self, owner: str, repo: str, pr_number: int) -> List[Dict]:
        """获取 PR 的 reviews"""
        reviews = []
        page = 1
        
        while True:
            url = f"{self.base_url}/repos/{owner}/{repo}/pulls/{pr_number}/reviews"
            params = {'per_page': 100, 'page': page}
            
            data = self.make_request(url, params)
            if not data:
                break
            
            reviews.extend(data)
            
            if len(data) < 100:
                break
            
            page += 1
            time.sleep(0.2)
        
        return reviews
    
    def get_pr_review_comments(self, owner: str, repo: str, pr_number: int) -> List[Dict]:
        """获取 PR 的 review comments"""
        review_comments = []
        page = 1
        
        while True:
            url = f"{self.base_url}/repos/{owner}/{repo}/pulls/{pr_number}/comments"
            params = {'per_page': 100, 'page': page}
            
            data = self.make_request(url, params)
            if not data:
                break
            
            review_comments.extend(data)
            
            if len(data) < 100:
                break
            
            page += 1
            time.sleep(0.2)
        
        return review_comments
    
    def get_commit_details(self, owner: str, repo: str, sha: str) -> Optional[Dict]:
        """获取 commit 的详细信息"""
        url = f"{self.base_url}/repos/{owner}/{repo}/commits/{sha}"
        return self.make_request(url)
    
    def process_pr_data(self, repo_info: Dict, pr_data: Dict, language: str) -> bool:
        """处理单个 PR 的完整数据，保存到JSONL文件"""
        owner = repo_info['owner']['login']
        repo_name = repo_info['name']
        repo_full_name = repo_info['full_name']
        pr_number = pr_data['number']
        
        logger.info(f"Processing PR #{pr_number} in {repo_full_name}")
        
        try:
            # 获取 PR 相关数据
            commits = self.get_pr_commits(owner, repo_name, pr_number)
            files = self.get_pr_files(owner, repo_name, pr_number)
            reviews = self.get_pr_reviews(owner, repo_name, pr_number)
            review_comments = self.get_pr_review_comments(owner, repo_name, pr_number)
            
            if not commits:
                logger.warning(f"No commits found for PR #{pr_number}")
                return False
            
            # 计算统计信息
            stats = {
                'additions': sum([f.get('additions', 0) for f in files]),
                'deletions': sum([f.get('deletions', 0) for f in files]),
                'changed_files': len(files),
                'commits_count': len(commits),
                'reviews_count': len(reviews),
                'review_comments_count': len(review_comments)
            }
            
            # 保存PR基本信息
            pr_record = {
                'repo_full_name': repo_full_name,
                'repo_language': repo_info.get('language'),
                'repo_stars': repo_info.get('stargazers_count', 0),
                'pr_number': pr_number,
                'pr_title': pr_data.get('title', ''),
                'pr_body': pr_data.get('body', ''),
                'pr_author': pr_data.get('user', {}).get('login', ''),
                'pr_created_at': pr_data.get('created_at'),
                'pr_merged_at': pr_data.get('merged_at'),
                'pr_stats': stats,
                'processed_at': datetime.now().isoformat()
            }
            
            self.save_to_jsonl(pr_record, self.output_dir / f"{language}_pr_data.jsonl")
            
            # 保存审查评论
            for review in reviews:
                if review.get('body'):
                    review_record = {
                        'repo_full_name': repo_full_name,
                        'pr_number': pr_number,
                        'comment_type': 'review',
                        'reviewer': review.get('user', {}).get('login', ''),
                        'comment_text': review.get('body', ''),
                        'state': review.get('state', ''),
                        'created_at': review.get('submitted_at')
                    }
                    self.save_to_jsonl(review_record, self.output_dir / f"{language}_review_comments.jsonl")
            
            for comment in review_comments:
                if comment.get('body'):
                    comment_record = {
                        'repo_full_name': repo_full_name,
                        'pr_number': pr_number,
                        'comment_type': 'review_comment',
                        'reviewer': comment.get('user', {}).get('login', ''),
                        'comment_text': comment.get('body', ''),
                        'file_path': comment.get('path', ''),
                        'line_number': comment.get('line'),
                        'created_at': comment.get('created_at')
                    }
                    self.save_to_jsonl(comment_record, self.output_dir / f"{language}_review_comments.jsonl")
            
            # 处理每个 commit
            for commit in commits:
                commit_hash = commit['sha']
                
                commit_details = self.get_commit_details(owner, repo_name, commit_hash)
                if not commit_details:
                    continue
                
                # 保存commit信息
                commit_record = {
                    'repo_full_name': repo_full_name,
                    'pr_number': pr_number,
                    'commit_hash': commit_hash,
                    'commit_message': commit.get('commit', {}).get('message', ''),
                    'commit_author': commit.get('commit', {}).get('author', {}).get('name', ''),
                    'commit_author_email': commit.get('commit', {}).get('author', {}).get('email', ''),
                    'committed_at': commit.get('commit', {}).get('committer', {}).get('date'),
                    'commit_stats': commit_details.get('stats', {})
                }
                
                self.save_to_jsonl(commit_record, self.output_dir / f"{language}_commits.jsonl")
                
                # 处理文件变更
                commit_files = commit_details.get('files', [])
                for file_data in commit_files:
                    file_path = file_data.get('filename', '')
                    
                    # 判断文件的编程语言
                    file_language = self.analyzer.get_language_from_file(file_path)
                    
                    # 保存文件变更信息
                    file_change_record = {
                        'repo_full_name': repo_full_name,
                        'pr_number': pr_number,
                        'commit_hash': commit_hash,
                        'file_path': file_path,
                        'file_language': file_language,
                        'change_type': file_data.get('status', 'modified'),
                        'additions': file_data.get('additions', 0),
                        'deletions': file_data.get('deletions', 0),
                        'changes': file_data.get('changes', 0),
                        'patch_content': file_data.get('patch', '')
                    }
                    
                    self.save_to_jsonl(file_change_record, self.output_dir / f"{language}_file_changes.jsonl")
                    
                    # 如果文件有对应的语言，进行语义分析
                    if file_language:
                        patch_content = file_data.get('patch', '')
                        if patch_content:
                            # 分析函数变更
                            functions = self.analyzer.detect_function_changes(patch_content, file_language)
                            for func in functions:
                                func_record = {
                                    'repo_full_name': repo_full_name,
                                    'pr_number': pr_number,
                                    'commit_hash': commit_hash,
                                    'file_path': file_path,
                                    'file_language': file_language,
                                    'function_name': func['function_name'],
                                    'change_type': func['change_type'],
                                    'line_content': func['line_content'],
                                    'source': func['source']
                                }
                                self.save_to_jsonl(func_record, self.output_dir / f"{language}_function_changes.jsonl")
                                self.stats['functions_detected'] += 1
                            
                            # 分析类变更
                            classes = self.analyzer.detect_class_changes(patch_content, file_language)
                            for cls in classes:
                                class_record = {
                                    'repo_full_name': repo_full_name,
                                    'pr_number': pr_number,
                                    'commit_hash': commit_hash,
                                    'file_path': file_path,
                                    'file_language': file_language,
                                    'class_name': cls['class_name'],
                                    'change_type': cls['change_type'],
                                    'line_content': cls['line_content'],
                                    'source': cls['source']
                                }
                                self.save_to_jsonl(class_record, self.output_dir / f"{language}_class_changes.jsonl")
                                self.stats['classes_detected'] += 1
                            
                            # 保存diff hunks
                            hunks = self.analyzer.parse_diff_hunks(patch_content)
                            for i, hunk in enumerate(hunks):
                                hunk_record = {
                                    'repo_full_name': repo_full_name,
                                    'pr_number': pr_number,
                                    'commit_hash': commit_hash,
                                    'file_path': file_path,
                                    'file_language': file_language,
                                    'hunk_index': i,
                                    'old_start': hunk['old_start'],
                                    'old_count': hunk['old_count'],
                                    'new_start': hunk['new_start'],
                                    'new_count': hunk['new_count'],
                                    'context': hunk['context'],
                                    'content': '\n'.join(hunk['changes'])
                                }
                                self.save_to_jsonl(hunk_record, self.output_dir / f"{language}_diff_hunks.jsonl")
                        
                        # 提取import信息
                        file_content = self.get_file_content_at_commit(owner, repo_name, file_path, commit_hash)
                        if file_content:
                            imports = self.analyzer.extract_imports(file_content, file_language)
                            for imp in imports:
                                import_record = {
                                    'repo_full_name': repo_full_name,
                                    'pr_number': pr_number,
                                    'commit_hash': commit_hash,
                                    'file_path': file_path,
                                    'file_language': file_language,
                                    'import_statement': imp['import_statement'],
                                    'import_type': imp['import_type'],
                                    'module_name': imp['module_name'],
                                    'imported_items': imp.get('imported_items'),
                                    'line_number': imp['line_number']
                                }
                                self.save_to_jsonl(import_record, self.output_dir / f"{language}_imports.jsonl")
                                self.stats['imports_extracted'] += 1
                    
                    # 更新语言统计
                    if file_language:
                        if file_language not in self.stats['language_stats']:
                            self.stats['language_stats'][file_language] = 0
                        self.stats['language_stats'][file_language] += 1
                
                time.sleep(0.3)
            
            logger.info(f"Successfully processed PR #{pr_number} with {len(commits)} commits")
            return True
            
        except Exception as e:
            logger.error(f"Error processing PR #{pr_number}: {e}")
            return False
    
    def crawl_language(self, language: str, target_repos: int = 200, max_prs_per_repo: int = None):
        """爬取指定语言的数据并保存到JSONL文件"""
        logger.info(f"Starting crawl for language: {language}, target repos: {target_repos}")
        
        # 获取已处理的仓库集合
        processed_repos_set = self.get_processed_repos_set(language)
        
        successfully_processed_repos = len(processed_repos_set)
        logger.info(f"Resume from previous progress: {successfully_processed_repos} repos already processed")
        logger.info(f"Still need to process: {target_repos - successfully_processed_repos} repos")
        
        if successfully_processed_repos >= target_repos:
            logger.info(f"Target already reached! {successfully_processed_repos}/{target_repos} repos processed.")
            return
        
        current_start_index = 0
        batch_size = 50
        
        while successfully_processed_repos < target_repos:
            repos_batch = self.load_repos_from_file(
                language, 
                start_index=current_start_index, 
                batch_size=batch_size
            )
            
            if not repos_batch:
                logger.warning(f"No more repositories available. Processed {successfully_processed_repos}/{target_repos} repos.")
                break
            
            logger.info(f"Loaded batch of {len(repos_batch)} repos")
            logger.info(f"Progress: {successfully_processed_repos}/{target_repos} repos completed")
            
            for repo_idx, repo in enumerate(repos_batch):
                if successfully_processed_repos >= target_repos:
                    break
                
                try:
                    repo_full_name = f"{repo['owner']['login']}/{repo['name']}"
                    
                    # 检查是否已处理过
                    if repo_full_name in processed_repos_set:
                        logger.info(f"Skipping already processed repo: {repo_full_name}")
                        self.stats['repos_skipped_already_processed'] += 1
                        continue
                    
                    self.stats['total_repos_attempted'] += 1
                    logger.info(f"Processing repo {repo_idx + 1}/{len(repos_batch)}: {repo_full_name}")
                    
                    # 获取PR列表
                    prs = self.get_pull_requests(
                        repo['owner']['login'], 
                        repo['name'], 
                        max_prs=max_prs_per_repo,
                        skip_if_too_many=200
                    )
                    
                    if not prs:
                        logger.warning(f"No PRs found for {repo_full_name}")
                        self.stats['repos_skipped_no_prs'] += 1
                        continue
                    
                    logger.info(f"Processing {len(prs)} PRs for {repo_full_name}")
                    
                    # 处理所有PR
                    repo_pr_count = 0
                    for pr_idx, pr in enumerate(prs):
                        logger.info(f"Processing PR {pr_idx + 1}/{len(prs)} (#{pr['number']})")
                        
                        if self.process_pr_data(repo, pr, language):
                            repo_pr_count += 1
                            self.stats['total_prs_processed'] += 1
                        
                        time.sleep(1)
                    
                    # 只要成功处理了PR，就计入成功数量
                    if repo_pr_count > 0:
                        successfully_processed_repos += 1
                        processed_repos_set.add(repo_full_name)
                        self.stats['repos_successfully_processed'] += 1
                        logger.info(f"✅ Completed repo {repo_full_name}: processed {repo_pr_count} PRs")
                        logger.info(f"🎯 Progress: {successfully_processed_repos}/{target_repos} repos completed")
                    
                except Exception as e:
                    logger.error(f"Error processing repo {repo['name']}: {e}")
                    self.stats['repos_failed'] += 1
                    continue
            
            current_start_index += batch_size
            
            if successfully_processed_repos >= target_repos:
                break
            
            if len(repos_batch) < batch_size:
                break
        
        logger.info(f"Completed crawling {language}. Successfully processed {successfully_processed_repos}/{target_repos} repositories")
    
    def run(self, languages: List[str], target_repos_per_language: int = 200, max_prs_per_repo: int = None):
        """运行爬虫"""
        logger.info(f"Starting multi-language GitHub PR crawler for languages: {languages}")
        
        for language in languages:
            try:
                self.crawl_language(language, target_repos_per_language, max_prs_per_repo)
                logger.info(f"Completed language: {language}")
                time.sleep(5)
            except Exception as e:
                logger.error(f"Failed to crawl language {language}: {e}")
                continue
        
        logger.info(f"Crawling completed. Total API calls: {self.api_calls}")
        self.print_statistics()
    
    def print_statistics(self):
        """打印统计信息"""
        logger.info("=== Multi-Language Crawling Statistics ===")
        logger.info(f"Total API calls: {self.api_calls}")
        logger.info(f"Remaining rate limit: {self.rate_limit_remaining}")
        logger.info(f"Total repos attempted: {self.stats['total_repos_attempted']}")
        logger.info(f"Repos successfully processed: {self.stats['repos_successfully_processed']}")
        logger.info(f"Repos skipped (already processed): {self.stats['repos_skipped_already_processed']}")
        logger.info(f"Repos skipped (no PRs): {self.stats['repos_skipped_no_prs']}")
        logger.info(f"Repos skipped (too many PRs): {self.stats['repos_skipped_too_many_prs']}")
        logger.info(f"Repos failed: {self.stats['repos_failed']}")
        logger.info(f"Total PRs processed: {self.stats['total_prs_processed']}")
        logger.info(f"Functions detected: {self.stats['functions_detected']}")
        logger.info(f"Classes detected: {self.stats['classes_detected']}")
        logger.info(f"Imports extracted: {self.stats['imports_extracted']}")
        logger.info(f"Language distribution: {self.stats['language_stats']}")

def main():
    """主函数"""
    # 加载环境变量
    env_file = Path('.env')
    if env_file.exists():
        with open(env_file, 'r') as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value
    
    github_token = os.getenv('GITHUB_TOKEN')
    if not github_token:
        print("请设置环境变量 GITHUB_TOKEN")
        return
    
    # 要爬取的编程语言（支持多种语言）
    languages = [
       'python',
       # 'javascript',
       # 'java',
       # 'typescript',
       # 'golang',
       # 'cpp'
    ]
    
    # 创建多语言爬虫实例
    crawler = MultiLanguageGitHubPRCrawler(
        token=github_token, 
        repos_dir="top_2000_star_repos_this_year",
        output_dir="github_pr_data"
    )
    
    # 开始爬取
    crawler.run(
        languages=languages,
        target_repos_per_language=100,
        max_prs_per_repo=None
    )

if __name__ == "__main__":
    main()