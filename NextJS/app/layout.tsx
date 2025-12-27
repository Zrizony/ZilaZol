import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Supers Next.js App",
  description: "Next.js + Prisma + PostgreSQL foundation",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}


