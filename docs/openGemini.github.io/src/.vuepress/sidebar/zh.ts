import {sidebar} from "vuepress-theme-hope";

export const zhSidebar = sidebar({
    '/zh/': [
        {
            text: "用户指南",
            prefix: "guide/",
            children: [
                "introduction/",
                "quick_start/",
                "geminiql/",
                "write/",
                "manage/",
                "platforms/",
                "reference/",
                "versions/",
                "troubleshoot/",
            ],
        },
        {
            text: "开发指南",
            prefix: "dev-guide/",
            children: [
                "get_started/",
                "contribute/",
            ],
        },
        // {
        //     text: "在k8s中部署",
        //     prefix: "deploy-on-k8s/",
        //     children: [
        //     ],
        // },
    ],

    // separate file directory
    "/zh/guide/": "structure",
    "/zh/dev-guide/": "structure",
    "/zh/deploy-on-k8s/": "structure",

//  "/zh/config/": "structure",

//  "/zh/cookbook/": "structure",

});
