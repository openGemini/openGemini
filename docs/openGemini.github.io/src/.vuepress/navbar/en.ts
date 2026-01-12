import {navbar} from "vuepress-theme-hope";

export const enNavbar = navbar([
    {text: "Guide", icon: "creative", link: "/guide/quick_start/get_started"},
    {text: "Development", icon: "code", link: "/dev-guide/get_started/build_source_code"},
    {text: "openGemini on k8s", icon: "app", link: "/deploy-on-k8s/about/introduction"},
//  {
//    text: "Guide",
//    icon: "creative",
//    prefix: "/guide/",
//    children: [
//      {
//        text: "Bar",
//        icon: "creative",
//        prefix: "bar/",
//        children: ["baz", { text: "...", icon: "more", link: "" }],
//      },
//      {
//        text: "Foo",
//        icon: "config",
//        prefix: "foo/",
//        children: ["ray", { text: "...", icon: "more", link: "" }],
//      },
//    ],
//  },
//  {
//    text: "V2 Docs",
//    icon: "note",
//    link: "https://theme-hope.vuejs.press/",
//  },
]);
