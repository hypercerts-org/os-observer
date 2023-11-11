// @ts-check

/**
 * @type {import('next').NextConfig}
 **/
const nextConfig = {
  //output: 'export',
  productionBrowserSourceMaps: true,
  experimental: {
    serverComponentsExternalPackages: [
      "typeorm",
      "@opensource-observer/indexer",
    ],
  },
  webpack: (config, { isServer }) => {
    if (isServer) {
      config.plugins = [...config.plugins];
    }

    return config;
  },
  async redirects() {
    return [
      {
        source: "/discord",
        destination: "https://discord.com/invite/NGEJ35aWsq",
        permanent: false,
      },
      {
        source: "/docs",
        destination:
          "https://github.com/opensource-observer/oso/tree/main/docs",
        permanent: false,
      },
      {
        source: "/forms/kariba-interest",
        destination:
          "https://docs.google.com/forms/d/e/1FAIpQLSc5h3lczif2kjzDosfwdjsY6CdHDT8qHXbphZvNLwiT1uMHJw/viewform?usp=sf_link",
        permanent: false,
      },
      {
        source: "/forms/oso-data-collective",
        destination:
          "https://docs.google.com/forms/d/e/1FAIpQLSdfUwuzE5_n9ddfjIGel8LCyx84lU30vpmchooIGdPkTw9NuA/viewform?usp=sf_link",
        permanent: false,
      },
    ];
  },
};

module.exports = nextConfig;
