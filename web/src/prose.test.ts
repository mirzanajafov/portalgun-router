import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import ts from 'typescript'
import { describe, expect, it } from 'vitest'

const root = import.meta.dirname

function textIn(file: string): string[] {
  const source = ts.createSourceFile(file, readFileSync(join(root, file), 'utf8'), ts.ScriptTarget.Latest)
  const texts: string[] = []
  const visit = (node: ts.Node) => {
    if (ts.isJsxText(node) || ts.isStringLiteral(node) || ts.isTemplateLiteralToken(node)) texts.push(node.text)
    ts.forEachChild(node, visit)
  }
  visit(source)
  return texts
}

describe('UI text', () => {
  it('joins no sentences with semicolons or em dashes', () => {
    const files = readdirSync(root, { recursive: true, encoding: 'utf8' }).filter((f) => /\.tsx?$/.test(f) && !f.includes('.test.'))
    const offending = files.flatMap((f) =>
      textIn(f)
        .filter((t) => /;|\u2014/.test(t.replace(/&(mdash|#8212|#x2014);/gi, '\u2014').replace(/&#?\w+;/g, '')))
        .map((t) => `${f}: ${t.trim()}`),
    )
    expect(offending).toEqual([])
  })
})
