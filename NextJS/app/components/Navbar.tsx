'use client';

import Link from 'next/link';
import { useLanguage } from '../contexts/LanguageContext';

export default function Navbar() {
  const { language, setLanguage, t } = useLanguage();
  const isRTL = language === 'he';

  return (
    <nav className="navbar">
      <div className="navbar-container">
        <Link href="/" className="navbar-logo">
          {t('hero.title')}
        </Link>
        <div className="navbar-right">
          <div className="navbar-links">
            <Link href="/">{t('nav.home')}</Link>
            <Link href="/stores">{t('nav.stores')}</Link>
            <Link href="/products">{t('nav.products')}</Link>
            <Link href="/about">{t('nav.about')}</Link>
          </div>
          <div className="language-switcher">
            <button
              onClick={() => setLanguage('he')}
              className={`lang-button ${language === 'he' ? 'active' : ''}`}
              aria-label="עברית"
            >
              עברית
            </button>
            <span className="lang-separator">|</span>
            <button
              onClick={() => setLanguage('en')}
              className={`lang-button ${language === 'en' ? 'active' : ''}`}
              aria-label="English"
            >
              English
            </button>
          </div>
        </div>
      </div>
    </nav>
  );
}
