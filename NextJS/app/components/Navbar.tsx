import Link from 'next/link';

export default function Navbar() {
  return (
    <nav className="navbar">
      <div className="navbar-container">
        <Link href="/" className="navbar-logo">
          Supers
        </Link>
        <div className="navbar-links">
          <Link href="/">Home</Link>
          <Link href="/stores">Stores</Link>
          <Link href="/products">Products</Link>
          <Link href="/about">About</Link>
        </div>
      </div>
    </nav>
  );
}

