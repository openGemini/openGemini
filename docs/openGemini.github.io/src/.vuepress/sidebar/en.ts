import {sidebar} from "vuepress-theme-hope";

export const enSidebar = sidebar({
    "/": [
        {
            text: "Guide",
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
            text: "Development Guide",
            prefix: "dev-guide/",
            children: [
                "get_started/",
                "contribute/",
            ],
        },
//    {
//      text: "Docs",
//      icon: "note",
//      prefix: "guide/",
//      link: "demo/",
//      children: "structure",
//    },
//    "slides",
    ],

    // separate file directory
    "/guide/": "structure",
    "/dev-guide/": "structure",
    "/deploy-on-k8s/": "structure",

//  "/zh/config/": "structure",

//  "/zh/cookbook/": "structure",
});
