'use client';

import { useState } from 'react';
import { useLanguage } from '../contexts/LanguageContext';

interface ProductResult {
  productId: number;
  productName: string | null;
  barcode: string;
  brand: string | null;
  quantity: number | null;
  unit: string | null;
  imageUrl: string | null;
  prices: Array<{
    retailerName: string;
    retailerSlug: string;
    storeName: string | null;
    price: number;
    isOnSale: boolean;
    timestamp: string;
  }>;
}

export default function SearchBar() {
  const { t } = useLanguage();
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<ProductResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;

    setLoading(true);
    setError(null);

    try {
      const response = await fetch(`/api/products/search?q=${encodeURIComponent(query)}`);
      if (!response.ok) {
        throw new Error('Search failed');
      }
      const data = await response.json();
      setResults(data);
    } catch (err) {
      setError(t('search.error'));
      setResults([]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="search-section">
      <form onSubmit={handleSearch} className="search-form">
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder={t('search.placeholder')}
          className="search-input"
        />
        <button type="submit" className="search-button" disabled={loading}>
          {loading ? t('search.searching') : t('search.button')}
        </button>
      </form>

      {error && (
        <div className="search-error">
          {error}
        </div>
      )}

      {results.length > 0 && (
        <div className="results-container">
          <div className="results-header">
            <h2 className="results-title">{t('search.results')}</h2>
            <div className="results-count">{results.length} {results.length === 1 ? 'product' : 'products'}</div>
          </div>
          <div className="products-grid">
            {results.map((product) => {
              // Find best price (lowest)
              const sortedPrices = [...product.prices].sort((a, b) => a.price - b.price);
              const bestPrice = sortedPrices[0];
              const hasMultiplePrices = product.prices.length > 1;
              
              return (
                <div key={product.productId} className="product-card-modern">
                  {/* Product Image */}
                  <div className="product-image-wrapper">
                    {product.imageUrl ? (
                      <img 
                        src={product.imageUrl} 
                        alt={product.productName || product.barcode}
                        className="product-image-modern"
                        onError={(e) => {
                          e.currentTarget.src = 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="200" height="200"%3E%3Crect fill="%23f0f0f0" width="200" height="200"/%3E%3Ctext fill="%23999" font-family="sans-serif" font-size="14" x="50%25" y="50%25" text-anchor="middle" dominant-baseline="middle"%3ENo Image%3C/text%3E%3C/svg%3E';
                        }}
                      />
                    ) : (
                      <div className="product-image-placeholder">
                        <svg width="60" height="60" viewBox="0 0 24 24" fill="none" stroke="currentColor">
                          <rect x="3" y="3" width="18" height="18" rx="2" strokeWidth="2"/>
                          <path d="M9 9h6v6H9z" strokeWidth="2"/>
                        </svg>
                      </div>
                    )}
                    {bestPrice?.isOnSale && (
                      <div className="sale-badge">{t('product.onSale')}</div>
                    )}
                  </div>

                  {/* Product Info */}
                  <div className="product-info-modern">
                    <h3 className="product-name-modern">
                      {product.productName || `Product ${product.barcode}`}
                    </h3>
                    
                    <div className="product-meta">
                      {product.brand && (
                        <span className="product-brand-modern">{product.brand}</span>
                      )}
                      {product.quantity && product.unit && (
                        <span className="product-size-modern">
                          {product.quantity} {product.unit}
                        </span>
                      )}
                    </div>

                    {/* Best Price Highlight */}
                    {bestPrice && (
                      <div className="price-section">
                        <div className="best-price">
                          <span className="price-label">{t('product.bestPrice')}</span>
                          <span className="price-value-best">
                            ₪{bestPrice.price.toFixed(2)}
                          </span>
                          <span className="retailer-name-best">{bestPrice.retailerName}</span>
                        </div>
                        
                        {hasMultiplePrices && (
                          <div className="price-comparison">
                            <span className="price-range">
                              {sortedPrices.length} {t('product.priceRange')} ₪{sortedPrices[sortedPrices.length - 1].price.toFixed(2)} - ₪{bestPrice.price.toFixed(2)}
                            </span>
                          </div>
                        )}
                      </div>
                    )}

                    {/* All Prices (Collapsible) */}
                    {product.prices.length > 1 && (
                      <details className="price-details">
                        <summary className="price-details-summary">
                          {t('product.price')} ({product.prices.length})
                        </summary>
                        <div className="price-list">
                          {sortedPrices.map((price, idx) => (
                            <div key={idx} className={`price-item ${price.price === bestPrice.price ? 'price-item-best' : ''}`}>
                              <div className="price-item-retailer">{price.retailerName}</div>
                              <div className="price-item-store">{price.storeName || t('product.na')}</div>
                              <div className={`price-item-value ${price.isOnSale ? 'price-item-sale' : ''}`}>
                                ₪{price.price.toFixed(2)}
                                {price.isOnSale && <span className="sale-indicator">{t('product.onSale')}</span>}
                              </div>
                            </div>
                          ))}
                        </div>
                      </details>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      {results.length === 0 && !loading && query && !error && (
        <div className="no-results">
          {t('search.noResults')}
        </div>
      )}
    </div>
  );
}
