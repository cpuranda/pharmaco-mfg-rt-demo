/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      fontFamily: {
        sans: [
          'system-ui',
          '-apple-system',
          'Segoe UI',
          'Roboto',
          'Helvetica Neue',
          'Arial',
          'sans-serif',
        ],
        mono: ['ui-monospace', 'SFMono-Regular', 'Menlo', 'Consolas', 'monospace'],
      },
      colors: {
        sev: {
          critical: '#dc2626',
          warning: '#d97706',
          healthy: '#16a34a',
          watch: '#0284c7',
          degraded: '#ea580c',
        },
      },
    },
  },
  plugins: [],
}
