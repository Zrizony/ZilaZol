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
          <h2 className="results-title">{t('search.results')}</h2>
          {results.map((product) => (
            <div key={product.productId} className="product-card">
              <div className="product-header">
                <div className="product-info-row">
                  {product.imageUrl && (
                    <img 
                      src={product.imageUrl} 
                      alt={product.productName || product.barcode}
                      className="product-image"
                      onError={(e) => {
                        e.currentTarget.style.display = 'none';
                      }}
                    />
                  )}
                  <div className="product-details">
                    <h3 className="product-name">
                      {product.productName || `Product ${product.barcode}`}
                    </h3>
                    {product.brand && (
                      <span className="product-brand">
                        {t('product.brand')}: {product.brand}
                      </span>
                    )}
                    {product.quantity && product.unit && (
                      <span className="product-size">
                        {product.quantity} {product.unit}
                      </span>
                    )}
                  </div>
                </div>
              </div>

              {product.prices.length > 0 ? (
                <table className="price-table">
                  <thead>
                    <tr>
                      <th>{t('product.retailer')}</th>
                      <th>{t('product.store')}</th>
                      <th>{t('product.price')}</th>
                      <th>{t('product.status')}</th>
                      <th>{t('product.updated')}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {product.prices.map((price, idx) => (
                      <tr key={idx}>
                        <td className="retailer-cell">{price.retailerName}</td>
                        <td className="store-cell">
                          {price.storeName || t('product.na')}
                        </td>
                        <td className="price-cell">
                          <span className={price.isOnSale ? 'price-sale' : 'price-normal'}>
                            ₪{price.price.toFixed(2)}
                          </span>
                        </td>
                        <td className="status-cell">
                          {price.isOnSale ? (
                            <span className="badge-sale">{t('product.onSale')}</span>
                          ) : (
                            <span className="badge-normal">{t('product.regular')}</span>
                          )}
                        </td>
                        <td className="timestamp-cell">
                          {new Date(price.timestamp).toLocaleDateString()}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <p className="no-prices">{t('product.noPrices')}</p>
              )}
            </div>
          ))}
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
