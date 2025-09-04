import os
import json
import psycopg2
from pydantic import BaseModel
from typing import List
from datetime import datetime

# Read database credentials from environment variables
DATABASE_URL = os.getenv("DATABASE_URL")

def get_db_connection():
    """Get database connection"""
    conn = psycopg2.connect(DATABASE_URL)
    return conn

def init_db():
    """Initialize database with all tables"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Workflows table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS workflows (
            id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            comparison_mode TEXT NOT NULL DEFAULT 'full',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            progress INTEGER DEFAULT 0,
            current_stage TEXT,
            message TEXT,
            wi_document_path TEXT,
            item_master_path TEXT
        )
    ''')
    
    # Knowledge base table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS knowledge_base (
            id SERIAL PRIMARY KEY,
            material_name TEXT NOT NULL,
            part_number TEXT,
            description TEXT,
            classification_label INTEGER,
            confidence_level TEXT,
            supplier_info TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            workflow_id TEXT,
            approved_by TEXT,
            approved_at TIMESTAMP,
            metadata TEXT
        )
    ''')
    
    # Pending approvals table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS pending_approvals (
            id SERIAL PRIMARY KEY,
            workflow_id TEXT NOT NULL,
            item_data TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reviewed_by TEXT,
            reviewed_at TIMESTAMP,
            review_notes TEXT
        )
    ''')
    
    # Workflow results table
    cur.execute('''
        CREATE TABLE IF NOT EXISTS workflow_results (
            id SERIAL PRIMARY KEY,
            workflow_id TEXT NOT NULL,
            results_data TEXT NOT NULL,
            summary_data TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    cur.close()
    conn.close()

# Pydantic models for API request bodies
class ItemApprovalRequest(BaseModel):
    item_ids: List[int]

class WorkflowModel:
    @staticmethod
    def create_workflow(workflow_id, comparison_mode='full', wi_path=None, item_path=None):
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO workflows (id, comparison_mode, wi_document_path, item_master_path)
            VALUES (%s, %s, %s, %s)
        ''', (workflow_id, comparison_mode, wi_path, item_path))
        conn.commit()
        cur.close()
        conn.close()
    
    @staticmethod
    def update_workflow_status(workflow_id, status, progress=None, stage=None, message=None):
        conn = get_db_connection()
        cur = conn.cursor()
        updates = ['status = %s', 'updated_at = CURRENT_TIMESTAMP']
        values = [status]
        
        if progress is not None:
            updates.append('progress = %s')
            values.append(progress)
        if stage:
            updates.append('current_stage = %s')
            values.append(stage)
        if message:
            updates.append('message = %s')
            values.append(message)
        
        values.append(workflow_id)
        
        cur.execute(f'''
            UPDATE workflows SET {', '.join(updates)}
            WHERE id = %s
        ''', values)
        conn.commit()
        cur.close()
        conn.close()
    
    @staticmethod
    def get_workflow(workflow_id):
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('''
            SELECT * FROM workflows WHERE id = %s
        ''', (workflow_id,))
        workflow = cur.fetchone()
        cur.close()
        conn.close()
        return dict(zip([column[0] for column in cur.description], workflow)) if workflow else None
    
    @staticmethod
    def get_all_workflows(limit=50):
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('''
            SELECT * FROM workflows 
            ORDER BY created_at DESC 
            LIMIT %s
        ''', (limit,))
        workflows = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(zip([column[0] for column in cur.description], w)) for w in workflows]
    
class KnowledgeBaseModel:
    @staticmethod
    def add_item(material_name, part_number=None, description=None, 
                classification_label=None, confidence_level=None, 
                supplier_info=None, workflow_id=None, approved_by=None, metadata=None):
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO knowledge_base 
            (material_name, part_number, description, classification_label, 
             confidence_level, supplier_info, workflow_id, approved_by, 
             approved_at, metadata)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, %s)
        ''', (material_name, part_number, description, classification_label,
              confidence_level, supplier_info, workflow_id, approved_by, metadata))
        conn.commit()
        cur.close()
        conn.close()
    
    @staticmethod
    def search_items(query='', limit=50):
        conn = get_db_connection()
        cur = conn.cursor()
        if query:
            cur.execute('''
                SELECT * FROM knowledge_base 
                WHERE material_name ILIKE %s OR part_number ILIKE %s OR description ILIKE %s
                ORDER BY created_at DESC
                LIMIT %s
            ''', (f'%{query}%', f'%{query}%', f'%{query}%', limit))
        else:
            cur.execute('''
                SELECT * FROM knowledge_base 
                ORDER BY created_at DESC
                LIMIT %s
            ''', (limit,))
        items = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(zip([column[0] for column in cur.description], item)) for item in items]
    
    @staticmethod
    def get_stats():
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT COUNT(*) as count FROM knowledge_base')
        total_items = cur.fetchone()[0]
        cur.execute('''
            SELECT COUNT(DISTINCT workflow_id) as count FROM knowledge_base 
            WHERE workflow_id IS NOT NULL
        ''')
        total_workflows = cur.fetchone()[0]
        cur.execute('''
            SELECT COUNT(*) as count FROM knowledge_base 
            WHERE confidence_level = 'high'
        ''')
        high_confidence_items = cur.fetchone()[0]
        
        match_rate = (high_confidence_items / total_items * 100) if total_items > 0 else 0
        cur.close()
        conn.close()
        
        return {
            'total_items': total_items,
            'total_workflows': total_workflows,
            'total_matches': total_items, # Assuming total matches equals total items in KB
            'match_rate': round(match_rate, 1)
        }

class PendingApprovalModel:
    @staticmethod
    def add_pending_item(workflow_id, item_data):
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('''
            INSERT INTO pending_approvals (workflow_id, item_data)
            VALUES (%s, %s)
        ''', (workflow_id, item_data))
        conn.commit()
        cur.close()
        conn.close()
    
    @staticmethod
    def get_pending_items(workflow_id=None):
        conn = get_db_connection()
        cur = conn.cursor()
        if workflow_id:
            cur.execute('''
                SELECT * FROM pending_approvals 
                WHERE workflow_id = %s AND status = 'pending'
                ORDER BY created_at DESC
            ''', (workflow_id,))
        else:
            cur.execute('''
                SELECT * FROM pending_approvals 
                WHERE status = 'pending'
                ORDER BY created_at DESC
            ''')
        items = cur.fetchall()
        cur.close()
        conn.close()
        return [dict(zip([column[0] for column in cur.description], item)) for item in items]
    
    @staticmethod
    def update_approval_status(item_ids, status, reviewer=None, notes=None):
        conn = get_db_connection()
        cur = conn.cursor()
        placeholders = ','.join(['%s' for _ in item_ids])
        cur.execute(f'''
            UPDATE pending_approvals 
            SET status = %s, reviewed_by = %s, reviewed_at = CURRENT_TIMESTAMP, review_notes = %s
            WHERE id IN ({placeholders})
        ''', [status, reviewer, notes] + item_ids)
        conn.commit()
        cur.close()
        conn.close()