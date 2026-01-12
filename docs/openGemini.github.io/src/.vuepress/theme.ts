import { hopeTheme } from 'vuepress-theme-hope';
import { enNavbar, zhNavbar } from './navbar/index.js';
import { enSidebar, zhSidebar } from './sidebar/index.js';
// import { getDirname, path } from '@vuepress/utils';
import { getFooter } from './utils/getFooter.js';

// 获取当前文件目录
// const __dirname = getDirname(import.meta.url);

export default hopeTheme({
    hostname: 'https://openGemini.github.io',

    author: {
        name: 'openGemini',
        url: 'https://github.com/openGemini/openGemini',
    },

    pageInfo: ['Author', 'Original', 'Date', 'Category', 'Tag', 'ReadingTime', 'Word'],

    iconAssets: '/icon/iconfont.css',

    logo: 'images/logo.svg', // 首页左上角

    repo: 'openGemini/openGemini.github.io',

    docsDir: '/src',

    darkmode: 'toggle',

    locales: {
        '/': {
            // navbar
            navbar: enNavbar,

            // sidebar
            sidebar: enSidebar,

            copyright: 'Copyright @2023 openGemini-All Rights Reserved.',
            footer: getFooter('en'),

            displayFooter: true,

            metaLocales: {
                editLink: 'Edit this page on GitHub',
            },
        },

        /**
         * Chinese locale config
         */
        '/zh/': {
            // navbar
            navbar: zhNavbar,

            // sidebar
            sidebar: zhSidebar,

            copyright: 'Copyright @2023 openGemini-All Rights Reserved.',
            footer: getFooter('zh'),

            displayFooter: true,

            // page meta
            metaLocales: {
                editLink: '在 GitHub 上编辑此页',
            },
        },
    },

    encrypt: {
        config: {
            '/demo/encrypt.html': ['1234'],
            '/zh/demo/encrypt.html': ['1234'],
        },
    },

    plugins: {
        copyCode: {
            fancy: true,
            duration: 1000,
        },
        // copyCode: false,

        // all features are enabled for demo, only preserve features you need here
        mdEnhance: {
            align: true,
            attrs: true,
            chart: true,
            codetabs: true,
            demo: true,
            echarts: true,
            figure: true,
            flowchart: true,
            gfm: true,
            imgLazyload: true,
            imgSize: true,
            include: true,
            katex: true,
            mark: true,
            mermaid: true,
            playground: {
                presets: ['ts', 'vue'],
            },
            presentation: {
                plugins: ['highlight', 'math', 'search', 'notes', 'zoom'],
            },
            stylize: [
                {
                    matcher: 'Recommended',
                    replacer: ({ tag }) => {
                        if (tag === 'em')
                            return {
                                tag: 'Badge',
                                attrs: { type: 'tip' },
                                content: 'Recommended',
                            };
                    },
                },
            ],
            sub: true,
            sup: true,
            tabs: true,
            vPre: true,
            vuePlayground: true,
        },

        // uncomment these if you want a pwa
        pwa: {
            favicon: '/images/logo.svg',
        },
    },
});
