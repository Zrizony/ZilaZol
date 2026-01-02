import type { Metadata } from "next";
import "./globals.css";
import { LanguageProvider } from "./contexts/LanguageContext";

export const metadata: Metadata = {
  title: "סופרס - השוואת מחירים | Supers - Price Comparison",
  description: "השווה מחירים בין רשתות ומצא את המבצעים הטובים ביותר | Compare prices across retailers and find the best deals",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="he" dir="rtl">
      <body>
        <LanguageProvider>
          {children}
        </LanguageProvider>
      </body>
    </html>
  );
}


