import Navbar from './components/Navbar';
import Hero from './components/Hero';
import SearchBar from './components/SearchBar';

export default function Home() {
  return (
    <div className="home-page">
      <Navbar />
      <Hero />
      <main className="main-content">
        <SearchBar />
      </main>
    </div>
  );
}
