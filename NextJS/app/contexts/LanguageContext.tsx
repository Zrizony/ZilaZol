'use client';

import { createContext, useContext, useState, useEffect, ReactNode } from 'react';

type Language = 'he' | 'en';

interface LanguageContextType {
  language: Language;
  setLanguage: (lang: Language) => void;
  t: (key: string) => string;
}

const LanguageContext = createContext<LanguageContextType | undefined>(undefined);

const translations = {
  he: {
    // Navigation
    'nav.home': 'בית',
    'nav.stores': 'חנויות',
    'nav.products': 'מוצרים',
    'nav.about': 'אודות',
    'nav.language': 'שפה',
    
    // Hero
    'hero.title': 'סופרס',
    'hero.subtitle': 'השווה מחירים בין רשתות ומצא את המבצעים הטובים ביותר על המוצרים האהובים עליך',
    
    // Search
    'search.placeholder': 'חפש מוצר לפי שם או ברקוד...',
    'search.button': 'חפש',
    'search.searching': 'מחפש...',
    'search.error': 'חיפוש נכשל. נסה שוב.',
    'search.noResults': 'לא נמצאו מוצרים. נסה לחפש לפי שם מוצר או ברקוד.',
    'search.results': 'תוצאות חיפוש',
    
    // Product
    'product.brand': 'מותג',
    'product.size': 'גודל',
    'product.retailer': 'רשת',
    'product.store': 'חנות',
    'product.price': 'מחיר',
    'product.status': 'סטטוס',
    'product.updated': 'עודכן',
    'product.onSale': 'במבצע',
    'product.regular': 'רגיל',
    'product.noPrices': 'לא נמצאו מחירים למוצר זה',
    'product.na': 'לא זמין',
  },
  en: {
    // Navigation
    'nav.home': 'Home',
    'nav.stores': 'Stores',
    'nav.products': 'Products',
    'nav.about': 'About',
    'nav.language': 'Language',
    
    // Hero
    'hero.title': 'Supers',
    'hero.subtitle': 'Compare prices across retailers and find the best deals on your favorite products',
    
    // Search
    'search.placeholder': 'Search for a product by name or barcode...',
    'search.button': 'Search',
    'search.searching': 'Searching...',
    'search.error': 'Failed to search products. Please try again.',
    'search.noResults': 'No products found. Try searching by product name or barcode.',
    'search.results': 'Search Results',
    
    // Product
    'product.brand': 'Brand',
    'product.size': 'Size',
    'product.retailer': 'Retailer',
    'product.store': 'Store',
    'product.price': 'Price',
    'product.status': 'Status',
    'product.updated': 'Updated',
    'product.onSale': 'On Sale',
    'product.regular': 'Regular',
    'product.noPrices': 'No prices found for this product',
    'product.na': 'N/A',
  },
};

export function LanguageProvider({ children }: { children: ReactNode }) {
  const [language, setLanguageState] = useState<Language>('he');

  useEffect(() => {
    // Load language from localStorage or default to Hebrew
    const savedLang = localStorage.getItem('language') as Language;
    if (savedLang && (savedLang === 'he' || savedLang === 'en')) {
      setLanguageState(savedLang);
    }
  }, []);

  const setLanguage = (lang: Language) => {
    setLanguageState(lang);
    localStorage.setItem('language', lang);
    // Update HTML dir attribute
    document.documentElement.dir = lang === 'he' ? 'rtl' : 'ltr';
    document.documentElement.lang = lang;
  };

  const t = (key: string): string => {
    return translations[language][key as keyof typeof translations.he] || key;
  };

  // Set initial direction
  useEffect(() => {
    document.documentElement.dir = language === 'he' ? 'rtl' : 'ltr';
    document.documentElement.lang = language;
  }, [language]);

  return (
    <LanguageContext.Provider value={{ language, setLanguage, t }}>
      {children}
    </LanguageContext.Provider>
  );
}

export function useLanguage() {
  const context = useContext(LanguageContext);
  if (context === undefined) {
    throw new Error('useLanguage must be used within a LanguageProvider');
  }
  return context;
}

