import type { Metadata, Viewport } from 'next'
import Link from 'next/link'
import './globals.css'

export const metadata: Metadata = {
  title: 'PortalGun Router',
  description: 'A gateway that picks a dimension for every request, and shows its work.',
}

export const viewport: Viewport = {
  themeColor: [
    { media: '(prefers-color-scheme: light)', color: '#f6f7f4' },
    { media: '(prefers-color-scheme: dark)', color: '#0b0f0d' },
  ],
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen">
        <header className="mx-auto flex max-w-7xl items-center justify-between gap-4 px-4 pt-5 pb-3 sm:px-6">
          <Link href="/" className="flex items-center gap-2">
            <span className="inline-block h-6 w-5 rounded-[50%] bg-[radial-gradient(circle,#d9ffde_0%,#39d353_55%,#1f7a33_100%)]" aria-hidden />
            <span className="text-lg font-semibold tracking-tight">PortalGun Router</span>
          </Link>
          <nav className="flex gap-4 text-sm">
            <Link href="/" className="muted hover:text-[var(--ink)]">
              Live
            </Link>
            <Link href="/results" className="muted hover:text-[var(--ink)]">
              Results
            </Link>
          </nav>
        </header>
        <main className="mx-auto max-w-7xl px-4 pb-16 sm:px-6">{children}</main>
      </body>
    </html>
  )
}
