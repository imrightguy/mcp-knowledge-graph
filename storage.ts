import { promises as fs } from 'fs';
import path from 'path';
import Database from 'better-sqlite3';

export interface Entity {
  name: string;
  entityType: string;
  observations: string[];
}

export interface Relation {
  from: string;
  to: string;
  relationType: string;
}

export interface KnowledgeGraph {
  entities: Entity[];
  relations: Relation[];
}

export interface StorageBackend {
  loadGraph(): Promise<KnowledgeGraph>;
  saveGraph(graph: KnowledgeGraph): Promise<void>;
  migrateFromJSONL?(jsonlPath: string): Promise<void>;
  close?(): void;
}

const FILE_MARKER = {
  type: "_aim",
  source: "mcp-knowledge-graph"
};

/**
 * JSONL Storage Backend
 * Original implementation using JSONL files
 */
export class JSONLStorage implements StorageBackend {
  constructor(private filePath: string) {}

  async loadGraph(): Promise<KnowledgeGraph> {
    try {
      const data = await fs.readFile(this.filePath, "utf-8");
      const lines = data.split("\n").filter(line => line.trim() !== "");

      if (lines.length === 0) {
        return { entities: [], relations: [] };
      }

      // Check first line for our file marker
      const firstLine = JSON.parse(lines[0]!);
      if (firstLine.type !== "_aim" || firstLine.source !== "mcp-knowledge-graph") {
        throw new Error(`File ${this.filePath} does not contain required _aim safety marker.`);
      }

      // Process remaining lines (skip metadata)
      return lines.slice(1).reduce((graph: KnowledgeGraph, line) => {
        const item = JSON.parse(line);
        if (item.type === "entity") graph.entities.push(item as Entity);
        if (item.type === "relation") graph.relations.push(item as Relation);
        return graph;
      }, { entities: [], relations: [] });
    } catch (error) {
      if (error instanceof Error && 'code' in error && (error as any).code === "ENOENT") {
        return { entities: [], relations: [] };
      }
      throw error;
    }
  }

  async saveGraph(graph: KnowledgeGraph): Promise<void> {
    const lines = [
      JSON.stringify(FILE_MARKER),
      ...graph.entities.map(e => JSON.stringify({ type: "entity", ...e })),
      ...graph.relations.map(r => JSON.stringify({ type: "relation", ...r })),
    ];

    // Ensure directory exists
    await fs.mkdir(path.dirname(this.filePath), { recursive: true });
    await fs.writeFile(this.filePath, lines.join("\n"));
  }
}

/**
 * SQLite Storage Backend
 * New implementation using SQLite for better performance
 */
export class SQLiteStorage implements StorageBackend {
  private db: Database.Database;
  private initialized: boolean = false;

  constructor(private dbPath: string) {
    this.db = new Database(dbPath);
    this.db.pragma('journal_mode = WAL'); // Better concurrency
  }

  private async initialize(): Promise<void> {
    if (this.initialized) return;

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS metadata (
        key TEXT PRIMARY KEY,
        value TEXT
      );

      CREATE TABLE IF NOT EXISTS entities (
        name TEXT PRIMARY KEY,
        entity_type TEXT NOT NULL,
        created_at INTEGER DEFAULT (strftime('%s', 'now'))
      );

      CREATE TABLE IF NOT EXISTS observations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entity_name TEXT NOT NULL,
        content TEXT NOT NULL,
        FOREIGN KEY (entity_name) REFERENCES entities(name) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS relations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_entity TEXT NOT NULL,
        to_entity TEXT NOT NULL,
        relation_type TEXT NOT NULL,
        UNIQUE(from_entity, to_entity, relation_type),
        FOREIGN KEY (from_entity) REFERENCES entities(name) ON DELETE CASCADE,
        FOREIGN KEY (to_entity) REFERENCES entities(name) ON DELETE CASCADE
      );

      CREATE INDEX IF NOT EXISTS idx_obs_entity ON observations(entity_name);
      CREATE INDEX IF NOT EXISTS idx_relations_from ON relations(from_entity);
      CREATE INDEX IF NOT EXISTS idx_relations_to ON relations(to_entity);
    `);

    // Store metadata
    const stmt = this.db.prepare(`
      INSERT OR REPLACE INTO metadata (key, value) VALUES ('type', '_aim'), ('source', 'mcp-knowledge-graph')
    `);
    stmt.run();

    this.initialized = true;
  }

  async loadGraph(): Promise<KnowledgeGraph> {
    await this.initialize();

    // Load entities with observations
    const entityStmt = this.db.prepare(`
      SELECT name, entity_type, GROUP_CONCAT(content, '\x00') as obs_contents
      FROM entities e
      LEFT JOIN observations o ON e.name = o.entity_name
      GROUP BY e.name, e.entity_type
    `);

    const entities: Entity[] = entityStmt.all().map((row: any) => ({
      name: row.name,
      entityType: row.entity_type,
      observations: row.obs_contents ? row.obs_contents.split('\x00') : []
    }));

    // Load relations
    const relationStmt = this.db.prepare(`
      SELECT from_entity as from, to_entity as to, relation_type as relationType
      FROM relations
    `);

    const relations: Relation[] = relationStmt.all() as Relation[];

    return { entities, relations };
  }

  async saveGraph(graph: KnowledgeGraph): Promise<void> {
    await this.initialize();

    // Use transaction for atomicity
    const saveTx = this.db.transaction(() => {
      // Clear existing data
      this.db.prepare('DELETE FROM relations').run();
      this.db.prepare('DELETE FROM observations').run();
      this.db.prepare('DELETE FROM entities').run();

      // Insert entities
      const entityStmt = this.db.prepare(`
        INSERT INTO entities (name, entity_type) VALUES (?, ?)
      `);
      for (const entity of graph.entities) {
        entityStmt.run(entity.name, entity.entityType);
      }

      // Insert observations
      const obsStmt = this.db.prepare(`
        INSERT INTO observations (entity_name, content) VALUES (?, ?)
      `);
      for (const entity of graph.entities) {
        for (const obs of entity.observations) {
          obsStmt.run(entity.name, obs);
        }
      }

      // Insert relations
      const relStmt = this.db.prepare(`
        INSERT OR IGNORE INTO relations (from_entity, to_entity, relation_type) VALUES (?, ?, ?)
      `);
      for (const relation of graph.relations) {
        relStmt.run(relation.from, relation.to, relation.relationType);
      }
    });

    saveTx();
  }

  async migrateFromJSONL(jsonlPath: string): Promise<void> {
    await this.initialize();

    // Check if migration already done
    const checkStmt = this.db.prepare("SELECT value FROM metadata WHERE key = 'jsonl_migrated'");
    const migrated = checkStmt.get();
    if (migrated) {
      return; // Already migrated
    }

    // Load from JSONL
    const jsonlStorage = new JSONLStorage(jsonlPath);
    const graph = await jsonlStorage.loadGraph();

    // Save to SQLite
    await this.saveGraph(graph);

    // Mark as migrated
    const markStmt = this.db.prepare(`
      INSERT INTO metadata (key, value) VALUES ('jsonl_migrated', '1')
    `);
    markStmt.run();

    console.error(`Migrated ${graph.entities.length} entities and ${graph.relations.length} relations from ${jsonlPath} to SQLite`);
  }

  close(): void {
    this.db.close();
  }
}

/**
 * Factory function to create storage backend based on environment variable
 */
export function createStorageBackend(
  filePath: string,
  storageType: 'jsonl' | 'sqlite' = 'jsonl'
): StorageBackend {
  if (storageType === 'sqlite') {
    // Convert .jsonl path to .db path
    const dbPath = filePath.replace(/\.jsonl$/, '.db');
    return new SQLiteStorage(dbPath);
  } else {
    return new JSONLStorage(filePath);
  }
}

/**
 * Get storage backend type from environment variable
 */
export function getStorageType(): 'jsonl' | 'sqlite' {
  const storageType = process.env.STORAGE_BACKEND?.toLowerCase();
  if (storageType === 'sqlite') {
    return 'sqlite';
  }
  return 'jsonl'; // Default
}
