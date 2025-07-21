#!/usr/bin/env python3
"""
JSONL to SQLite Database Converter
将GitHub PR数据从JSONL文件转换为SQLite数据库

基于数据库设计图创建的转换脚本，支持多语言数据导入
"""

import sqlite3
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import hashlib

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('jsonl_to_sqlite.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class JSONLToSQLiteConverter:
    """JSONL文件到SQLite数据库的转换器"""
    
    def __init__(self, db_path: str = "github_pr_data.db", jsonl_dir: str = "github_pr_data"):
        """
        初始化转换器
        
        Args:
            db_path: SQLite数据库文件路径
            jsonl_dir: JSONL文件所在目录
        """
        self.db_path = Path(db_path)
        self.jsonl_dir = Path(jsonl_dir)
        self.conn = None
        
        # 统计信息
        self.stats = {
            'repositories': 0,
            'pull_requests': 0,
            'commits': 0,
            'file_changes': 0,
            'function_changes': 0,
            'class_changes': 0,
            'diff_hunks': 0,
            'file_imports': 0,
            'review_comments': 0
        }
    
    def connect_db(self):
        """连接到SQLite数据库"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")  # 启用外键约束
            logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def create_tables(self):
        """创建数据库表结构"""
        try:
            cursor = self.conn.cursor()
            
            # 1. repositories表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS repositories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner TEXT NOT NULL,
                    name TEXT NOT NULL,
                    full_name TEXT UNIQUE NOT NULL,
                    language TEXT,
                    stars INTEGER DEFAULT 0,
                    url TEXT,
                    created_at TIMESTAMP,
                    UNIQUE(owner, name)
                )
            """)
            
            # 2. pull_requests表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pull_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repo_id INTEGER NOT NULL,
                    pr_number INTEGER NOT NULL,
                    title TEXT,
                    body TEXT,
                    author TEXT,
                    state TEXT DEFAULT 'merged',
                    created_at TIMESTAMP,
                    merged_at TIMESTAMP,
                    additions INTEGER DEFAULT 0,
                    deletions INTEGER DEFAULT 0,
                    changed_files INTEGER DEFAULT 0,
                    commits_count INTEGER DEFAULT 0,
                    reviews_count INTEGER DEFAULT 0,
                    review_comments_count INTEGER DEFAULT 0,
                    processed_at TIMESTAMP,
                    FOREIGN KEY (repo_id) REFERENCES repositories(id),
                    UNIQUE(repo_id, pr_number)
                )
            """)
            
            # 3. commits表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS commits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    repo_id INTEGER NOT NULL,
                    pr_id INTEGER NOT NULL,
                    commit_hash TEXT NOT NULL,
                    message TEXT,
                    author TEXT,
                    author_email TEXT,
                    committed_at TIMESTAMP,
                    additions INTEGER DEFAULT 0,
                    deletions INTEGER DEFAULT 0,
                    total_changes INTEGER DEFAULT 0,
                    FOREIGN KEY (repo_id) REFERENCES repositories(id),
                    FOREIGN KEY (pr_id) REFERENCES pull_requests(id),
                    UNIQUE(repo_id, commit_hash)
                )
            """)
            
            # 4. file_changes表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    commit_id INTEGER NOT NULL,
                    file_path TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    file_language TEXT,
                    additions INTEGER DEFAULT 0,
                    deletions INTEGER DEFAULT 0,
                    changes INTEGER DEFAULT 0,
                    patch_content TEXT,
                    FOREIGN KEY (commit_id) REFERENCES commits(id)
                )
            """)
            
            # 5. function_changes表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS function_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_change_id INTEGER NOT NULL,
                    function_name TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    line_content TEXT,
                    source TEXT,
                    FOREIGN KEY (file_change_id) REFERENCES file_changes(id)
                )
            """)
            
            # 6. class_changes表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS class_changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_change_id INTEGER NOT NULL,
                    class_name TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    line_content TEXT,
                    source TEXT,
                    FOREIGN KEY (file_change_id) REFERENCES file_changes(id)
                )
            """)
            
            # 7. diff_hunks表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS diff_hunks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_change_id INTEGER NOT NULL,
                    hunk_index INTEGER NOT NULL,
                    old_start INTEGER NOT NULL,
                    old_count INTEGER NOT NULL,
                    new_start INTEGER NOT NULL,
                    new_count INTEGER NOT NULL,
                    context TEXT,
                    content TEXT,
                    FOREIGN KEY (file_change_id) REFERENCES file_changes(id)
                )
            """)
            
            # 8. file_imports表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_imports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_change_id INTEGER NOT NULL,
                    import_statement TEXT NOT NULL,
                    import_type TEXT,
                    module_name TEXT,
                    imported_items TEXT,
                    line_number INTEGER,
                    last_updated TIMESTAMP,
                    FOREIGN KEY (file_change_id) REFERENCES file_changes(id)
                )
            """)
            
            # 9. review_comments表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS review_comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pr_id INTEGER NOT NULL,
                    comment_type TEXT NOT NULL,
                    reviewer TEXT,
                    comment_text TEXT,
                    file_path TEXT,
                    line_number INTEGER,
                    state TEXT,
                    created_at TIMESTAMP,
                    FOREIGN KEY (pr_id) REFERENCES pull_requests(id)
                )
            """)
            
            # 创建索引以提高查询性能
            self._create_indexes(cursor)
            
            self.conn.commit()
            logger.info("Database tables created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    def _create_indexes(self, cursor):
        """创建数据库索引"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_repositories_full_name ON repositories(full_name)",
            "CREATE INDEX IF NOT EXISTS idx_pull_requests_repo_id ON pull_requests(repo_id)",
            "CREATE INDEX IF NOT EXISTS idx_pull_requests_pr_number ON pull_requests(pr_number)",
            "CREATE INDEX IF NOT EXISTS idx_commits_repo_id ON commits(repo_id)",
            "CREATE INDEX IF NOT EXISTS idx_commits_pr_id ON commits(pr_id)",
            "CREATE INDEX IF NOT EXISTS idx_commits_hash ON commits(commit_hash)",
            "CREATE INDEX IF NOT EXISTS idx_file_changes_commit_id ON file_changes(commit_id)",
            "CREATE INDEX IF NOT EXISTS idx_file_changes_language ON file_changes(file_language)",
            "CREATE INDEX IF NOT EXISTS idx_function_changes_file_id ON function_changes(file_change_id)",
            "CREATE INDEX IF NOT EXISTS idx_class_changes_file_id ON class_changes(file_change_id)",
            "CREATE INDEX IF NOT EXISTS idx_diff_hunks_file_id ON diff_hunks(file_change_id)",
            "CREATE INDEX IF NOT EXISTS idx_file_imports_file_id ON file_imports(file_change_id)",
            "CREATE INDEX IF NOT EXISTS idx_review_comments_pr_id ON review_comments(pr_id)"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
    
    def get_or_create_repository(self, repo_full_name: str, repo_data: Dict) -> int:
        """获取或创建repository记录，返回repository ID"""
        cursor = self.conn.cursor()
        
        # 首先尝试查找现有的repository
        cursor.execute("SELECT id FROM repositories WHERE full_name = ?", (repo_full_name,))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        
        # 如果不存在，创建新的repository
        owner, name = repo_full_name.split('/', 1)
        
        cursor.execute("""
            INSERT INTO repositories (owner, name, full_name, language, stars, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            owner,
            name,
            repo_full_name,
            repo_data.get('repo_language'),
            repo_data.get('repo_stars', 0),
            datetime.now().isoformat()
        ))
        
        repo_id = cursor.lastrowid
        self.stats['repositories'] += 1
        return repo_id
    
    def get_or_create_pull_request(self, repo_id: int, pr_data: Dict) -> int:
        """获取或创建pull request记录，返回PR ID"""
        cursor = self.conn.cursor()
        pr_number = pr_data['pr_number']
        
        # 首先尝试查找现有的PR
        cursor.execute("SELECT id FROM pull_requests WHERE repo_id = ? AND pr_number = ?", (repo_id, pr_number))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        
        # 如果不存在，创建新的PR
        pr_stats = pr_data.get('pr_stats', {})
        
        cursor.execute("""
            INSERT INTO pull_requests (
                repo_id, pr_number, title, body, author, state,
                created_at, merged_at, additions, deletions, changed_files,
                commits_count, reviews_count, review_comments_count, processed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            repo_id,
            pr_number,
            pr_data.get('pr_title'),
            pr_data.get('pr_body'),
            pr_data.get('pr_author'),
            'merged',
            pr_data.get('pr_created_at'),
            pr_data.get('pr_merged_at'),
            pr_stats.get('additions', 0),
            pr_stats.get('deletions', 0),
            pr_stats.get('changed_files', 0),
            pr_stats.get('commits_count', 0),
            pr_stats.get('reviews_count', 0),
            pr_stats.get('review_comments_count', 0),
            pr_data.get('processed_at')
        ))
        
        pr_id = cursor.lastrowid
        self.stats['pull_requests'] += 1
        return pr_id
    
    def get_or_create_commit(self, repo_id: int, pr_id: int, commit_data: Dict) -> int:
        """获取或创建commit记录，返回commit ID"""
        cursor = self.conn.cursor()
        commit_hash = commit_data['commit_hash']
        
        # 首先尝试查找现有的commit
        cursor.execute("SELECT id FROM commits WHERE repo_id = ? AND commit_hash = ?", (repo_id, commit_hash))
        result = cursor.fetchone()
        
        if result:
            return result[0]
        
        # 如果不存在，创建新的commit
        commit_stats = commit_data.get('commit_stats', {})
        
        cursor.execute("""
            INSERT INTO commits (
                repo_id, pr_id, commit_hash, message, author, author_email,
                committed_at, additions, deletions, total_changes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            repo_id,
            pr_id,
            commit_hash,
            commit_data.get('commit_message'),
            commit_data.get('commit_author'),
            commit_data.get('commit_author_email'),
            commit_data.get('committed_at'),
            commit_stats.get('additions', 0),
            commit_stats.get('deletions', 0),
            commit_stats.get('total', 0)
        ))
        
        commit_id = cursor.lastrowid
        self.stats['commits'] += 1
        return commit_id
    
    def process_jsonl_files(self, languages: List[str]):
        """处理所有JSONL文件"""
        for language in languages:
            logger.info(f"Processing {language} data...")
            
            # 处理顺序很重要，需要先处理依赖的表
            self._process_pr_data(language)
            self._process_commits(language)
            self._process_file_changes(language)
            self._process_function_changes(language)
            self._process_class_changes(language)
            self._process_diff_hunks(language)
            self._process_imports(language)
            self._process_review_comments(language)
            
            logger.info(f"Completed processing {language} data")
    
    def _process_pr_data(self, language: str):
        """处理PR数据文件"""
        file_path = self.jsonl_dir / f"{language}_pr_data.jsonl"
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return
        
        logger.info(f"Processing PR data from {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line.strip())
                    repo_id = self.get_or_create_repository(data['repo_full_name'], data)
                    self.get_or_create_pull_request(repo_id, data)
                    
                    if line_num % 100 == 0:
                        self.conn.commit()
                        logger.info(f"Processed {line_num} PR records")
                        
                except Exception as e:
                    logger.error(f"Error processing PR data line {line_num}: {e}")
                    continue
        
        self.conn.commit()
        logger.info(f"Completed processing PR data from {file_path}")
    
    def _process_commits(self, language: str):
        """处理commit数据文件"""
        file_path = self.jsonl_dir / f"{language}_commits.jsonl"
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return
        
        logger.info(f"Processing commits from {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line.strip())
                    
                    # 获取repo_id和pr_id
                    repo_id = self._get_repo_id(data['repo_full_name'])
                    pr_id = self._get_pr_id(repo_id, data['pr_number'])
                    
                    if repo_id and pr_id:
                        self.get_or_create_commit(repo_id, pr_id, data)
                    
                    if line_num % 100 == 0:
                        self.conn.commit()
                        logger.info(f"Processed {line_num} commit records")
                        
                except Exception as e:
                    logger.error(f"Error processing commit data line {line_num}: {e}")
                    continue
        
        self.conn.commit()
        logger.info(f"Completed processing commits from {file_path}")
    
    def _process_file_changes(self, language: str):
        """处理文件变更数据"""
        file_path = self.jsonl_dir / f"{language}_file_changes.jsonl"
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return
        
        logger.info(f"Processing file changes from {file_path}")
        cursor = self.conn.cursor()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line.strip())
                    
                    # 获取commit_id
                    commit_id = self._get_commit_id(data['repo_full_name'], data['commit_hash'])
                    
                    if commit_id:
                        cursor.execute("""
                            INSERT INTO file_changes (
                                commit_id, file_path, change_type, file_language,
                                additions, deletions, changes, patch_content
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            commit_id,
                            data['file_path'],
                            data['change_type'],
                            data.get('file_language'),
                            data.get('additions', 0),
                            data.get('deletions', 0),
                            data.get('changes', 0),
                            data.get('patch_content')
                        ))
                        
                        self.stats['file_changes'] += 1
                    
                    if line_num % 100 == 0:
                        self.conn.commit()
                        logger.info(f"Processed {line_num} file change records")
                        
                except Exception as e:
                    logger.error(f"Error processing file change data line {line_num}: {e}")
                    continue
        
        self.conn.commit()
        logger.info(f"Completed processing file changes from {file_path}")
    
    def _process_function_changes(self, language: str):
        """处理函数变更数据"""
        file_path = self.jsonl_dir / f"{language}_function_changes.jsonl"
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return
        
        logger.info(f"Processing function changes from {file_path}")
        cursor = self.conn.cursor()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line.strip())
                    
                    # 获取file_change_id
                    file_change_id = self._get_file_change_id(
                        data['repo_full_name'], 
                        data['commit_hash'], 
                        data['file_path']
                    )
                    
                    if file_change_id:
                        cursor.execute("""
                            INSERT INTO function_changes (
                                file_change_id, function_name, change_type, line_content, source
                            ) VALUES (?, ?, ?, ?, ?)
                        """, (
                            file_change_id,
                            data['function_name'],
                            data['change_type'],
                            data.get('line_content'),
                            data.get('source')
                        ))
                        
                        self.stats['function_changes'] += 1
                    
                    if line_num % 100 == 0:
                        self.conn.commit()
                        logger.info(f"Processed {line_num} function change records")
                        
                except Exception as e:
                    logger.error(f"Error processing function change data line {line_num}: {e}")
                    continue
        
        self.conn.commit()
        logger.info(f"Completed processing function changes from {file_path}")
    
    def _process_class_changes(self, language: str):
        """处理类变更数据"""
        file_path = self.jsonl_dir / f"{language}_class_changes.jsonl"
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return
        
        logger.info(f"Processing class changes from {file_path}")
        cursor = self.conn.cursor()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line.strip())
                    
                    # 获取file_change_id
                    file_change_id = self._get_file_change_id(
                        data['repo_full_name'], 
                        data['commit_hash'], 
                        data['file_path']
                    )
                    
                    if file_change_id:
                        cursor.execute("""
                            INSERT INTO class_changes (
                                file_change_id, class_name, change_type, line_content, source
                            ) VALUES (?, ?, ?, ?, ?)
                        """, (
                            file_change_id,
                            data['class_name'],
                            data['change_type'],
                            data.get('line_content'),
                            data.get('source')
                        ))
                        
                        self.stats['class_changes'] += 1
                    
                    if line_num % 100 == 0:
                        self.conn.commit()
                        logger.info(f"Processed {line_num} class change records")
                        
                except Exception as e:
                    logger.error(f"Error processing class change data line {line_num}: {e}")
                    continue
        
        self.conn.commit()
        logger.info(f"Completed processing class changes from {file_path}")
    
    def _process_diff_hunks(self, language: str):
        """处理diff hunks数据"""
        file_path = self.jsonl_dir / f"{language}_diff_hunks.jsonl"
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return
        
        logger.info(f"Processing diff hunks from {file_path}")
        cursor = self.conn.cursor()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line.strip())
                    
                    # 获取file_change_id
                    file_change_id = self._get_file_change_id(
                        data['repo_full_name'], 
                        data['commit_hash'], 
                        data['file_path']
                    )
                    
                    if file_change_id:
                        cursor.execute("""
                            INSERT INTO diff_hunks (
                                file_change_id, hunk_index, old_start, old_count,
                                new_start, new_count, context, content
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            file_change_id,
                            data['hunk_index'],
                            data['old_start'],
                            data['old_count'],
                            data['new_start'],
                            data['new_count'],
                            data.get('context'),
                            data.get('content')
                        ))
                        
                        self.stats['diff_hunks'] += 1
                    
                    if line_num % 100 == 0:
                        self.conn.commit()
                        logger.info(f"Processed {line_num} diff hunk records")
                        
                except Exception as e:
                    logger.error(f"Error processing diff hunk data line {line_num}: {e}")
                    continue
        
        self.conn.commit()
        logger.info(f"Completed processing diff hunks from {file_path}")
    
    def _process_imports(self, language: str):
        """处理导入数据"""
        file_path = self.jsonl_dir / f"{language}_imports.jsonl"
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return
        
        logger.info(f"Processing imports from {file_path}")
        cursor = self.conn.cursor()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line.strip())
                    
                    # 获取file_change_id
                    file_change_id = self._get_file_change_id(
                        data['repo_full_name'], 
                        data['commit_hash'], 
                        data['file_path']
                    )
                    
                    if file_change_id:
                        cursor.execute("""
                            INSERT INTO file_imports (
                                file_change_id, import_statement, import_type, module_name,
                                imported_items, line_number, last_updated
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            file_change_id,
                            data['import_statement'],
                            data.get('import_type'),
                            data.get('module_name'),
                            data.get('imported_items'),
                            data.get('line_number'),
                            datetime.now().isoformat()
                        ))
                        
                        self.stats['file_imports'] += 1
                    
                    if line_num % 100 == 0:
                        self.conn.commit()
                        logger.info(f"Processed {line_num} import records")
                        
                except Exception as e:
                    logger.error(f"Error processing import data line {line_num}: {e}")
                    continue
        
        self.conn.commit()
        logger.info(f"Completed processing imports from {file_path}")
    
    def _process_review_comments(self, language: str):
        """处理审查评论数据"""
        file_path = self.jsonl_dir / f"{language}_review_comments.jsonl"
        if not file_path.exists():
            logger.warning(f"File not found: {file_path}")
            return
        
        logger.info(f"Processing review comments from {file_path}")
        cursor = self.conn.cursor()
        
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    data = json.loads(line.strip())
                    
                    # 获取pr_id
                    repo_id = self._get_repo_id(data['repo_full_name'])
                    pr_id = self._get_pr_id(repo_id, data['pr_number'])
                    
                    if pr_id:
                        cursor.execute("""
                            INSERT INTO review_comments (
                                pr_id, comment_type, reviewer, comment_text,
                                file_path, line_number, state, created_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            pr_id,
                            data['comment_type'],
                            data.get('reviewer'),
                            data.get('comment_text'),
                            data.get('file_path'),
                            data.get('line_number'),
                            data.get('state'),
                            data.get('created_at')
                        ))
                        
                        self.stats['review_comments'] += 1
                    
                    if line_num % 100 == 0:
                        self.conn.commit()
                        logger.info(f"Processed {line_num} review comment records")
                        
                except Exception as e:
                    logger.error(f"Error processing review comment data line {line_num}: {e}")
                    continue
        
        self.conn.commit()
        logger.info(f"Completed processing review comments from {file_path}")
    
    def _get_repo_id(self, repo_full_name: str) -> Optional[int]:
        """根据仓库全名获取repository ID"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM repositories WHERE full_name = ?", (repo_full_name,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def _get_pr_id(self, repo_id: int, pr_number: int) -> Optional[int]:
        """根据仓库ID和PR号获取pull request ID"""
        if not repo_id:
            return None
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM pull_requests WHERE repo_id = ? AND pr_number = ?", (repo_id, pr_number))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def _get_commit_id(self, repo_full_name: str, commit_hash: str) -> Optional[int]:
        """根据仓库全名和commit hash获取commit ID"""
        repo_id = self._get_repo_id(repo_full_name)
        if not repo_id:
            return None
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM commits WHERE repo_id = ? AND commit_hash = ?", (repo_id, commit_hash))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def _get_file_change_id(self, repo_full_name: str, commit_hash: str, file_path: str) -> Optional[int]:
        """根据仓库全名、commit hash和文件路径获取file change ID"""
        commit_id = self._get_commit_id(repo_full_name, commit_hash)
        if not commit_id:
            return None
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM file_changes WHERE commit_id = ? AND file_path = ?", (commit_id, file_path))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def print_statistics(self):
        """打印转换统计信息"""
        logger.info("=== Conversion Statistics ===")
        for table, count in self.stats.items():
            logger.info(f"{table}: {count}")
        
        # 查询数据库中的实际记录数
        cursor = self.conn.cursor()
        tables = ['repositories', 'pull_requests', 'commits', 'file_changes', 
                 'function_changes', 'class_changes', 'diff_hunks', 'file_imports', 'review_comments']
        
        logger.info("=== Database Record Counts ===")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"{table}: {count}")
    
    def close(self):
        """关闭数据库连接"""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def run(self, languages: List[str]):
        """运行转换过程"""
        try:
            logger.info("Starting JSONL to SQLite conversion...")
            
            self.connect_db()
            self.create_tables()
            self.process_jsonl_files(languages)
            self.print_statistics()
            
            logger.info("Conversion completed successfully!")
            
        except Exception as e:
            logger.error(f"Conversion failed: {e}")
            raise
        finally:
            self.close()


def main():
    """主函数"""
    # 配置参数
    languages = ['javascript', 'python', 'java', 'typescript', 'golang', 'cpp']  # 可以根据需要调整
    
    converter = JSONLToSQLiteConverter(
        db_path="github_pr_data.db",
        jsonl_dir="github_pr_data"
    )
    
    converter.run(languages)


if __name__ == "__main__":
    main()
