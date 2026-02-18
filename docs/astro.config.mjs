// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  site: "https://xosnrdev.github.io",
  base: "/kifa",
  integrations: [
    starlight({
      title: "Kifa",
      description:
        "Crash-proof local logging for POS and mobile money systems.",
      favicon: "/favicon.svg",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/xosnrdev/kifa",
        },
      ],
      editLink: {
        baseUrl: "https://github.com/xosnrdev/kifa/edit/master/docs/",
      },
      customCss: ["./src/styles/custom.css"],
      head: [
        {
          tag: "meta",
          attrs: {
            property: "og:image",
            content: "https://xosnrdev.github.io/kifa/og.png",
          },
        },
        {
          tag: "meta",
          attrs: { property: "og:image:width", content: "1200" },
        },
        {
          tag: "meta",
          attrs: { property: "og:image:height", content: "630" },
        },
        {
          tag: "meta",
          attrs: { property: "og:image:type", content: "image/png" },
        },
        {
          tag: "meta",
          attrs: { property: "og:type", content: "website" },
        },
        {
          tag: "meta",
          attrs: { property: "og:site_name", content: "Kifa" },
        },
        {
          tag: "meta",
          attrs: { name: "twitter:card", content: "summary_large_image" },
        },
        {
          tag: "meta",
          attrs: {
            name: "twitter:image",
            content: "https://xosnrdev.github.io/kifa/og.png",
          },
        },
      ],
      sidebar: [
        {
          label: "Overview",
          items: [{ label: "Why Kifa", slug: "why-kifa" }],
        },
        {
          label: "Getting Started",
          items: [
            { label: "Installation", slug: "install" },
            { label: "Quick Start", slug: "quick-start" },
          ],
        },
        {
          label: "Guides",
          items: [
            { label: "Ingestion", slug: "guides/ingest" },
            { label: "Querying Logs", slug: "guides/querying" },
            { label: "Exporting Data", slug: "guides/exporting" },
            { label: "Choosing a Flush Mode", slug: "guides/flush-modes" },
            { label: "Configuration", slug: "guides/configuration" },
            { label: "Deployment", slug: "guides/deployment" },
          ],
        },
        {
          label: "Reference",
          items: [
            { label: "CLI Reference", slug: "reference/cli" },
            { label: "Configuration Reference", slug: "reference/config" },
            { label: "Changelog", slug: "reference/changelog" },
          ],
        },
        {
          label: "Internals",
          items: [{ label: "Architecture", slug: "internals/architecture" }],
        },
        {
          label: "Community",
          items: [
            { label: "Contributing", slug: "contributing" },
            { label: "Security Policy", slug: "security" },
          ],
        },
      ],
    }),
  ],
});
