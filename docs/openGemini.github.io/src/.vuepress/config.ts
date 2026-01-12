import { defineUserConfig } from 'vuepress';
import { searchProPlugin } from 'vuepress-plugin-search-pro';
import theme from './theme.js';
import { registerComponentsPlugin } from '@vuepress/plugin-register-components';
import {  path,getDirname } from '@vuepress/utils';

// 获取当前文件目录
const __dirname = getDirname(import.meta.url);

export default defineUserConfig({
    base: '/',

    locales: {
        '/': {
            lang: 'en-US',
            title: 'openGemini',
            description: 'openGemini docs site',
        },
        '/zh/': {
            lang: 'zh-CN',
            title: 'openGemini',
            description: 'openGemini 统一文档站点',
        },
    },

    theme,

    plugins: [
        searchProPlugin({
            // 索引全部内容
            indexContent: true,
            // 为分类和标签添加索引
            customFields: [
                //        {
                //          getter: (page) => page.frontmatter.category,
                //          formatter: "分类：$content",
                //        },
                //        {
                //          getter: (page) => page.frontmatter.tag,
                //          formatter: "标签：$content",
                //        },
            ],
            locales: {
                '/': {
                    placeholder: 'search',
                },

                '/zh/': {
                    placeholder: '开始搜索',
                },
            },
        }),
        // 注册自定义组件目录
        registerComponentsPlugin({
            componentsDir: path.resolve(__dirname, './components'),
        }),
    ],

    // Enable it with pwa
    // shouldPrefetch: false,
});
