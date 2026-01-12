const langMap = {
    en: {
        resource: 'Resources',
        roadmap: {
            name: 'Roadmap',
            link: '/guide/versions/roadmap.html',
        },
        devGuide: {
            name: 'Development Guide',
            link: '/dev-guide/get_started/build_source_code.html',
        },
        blog: 'Blog',
        support: 'Support',
        forum: 'Forum',
        contactUs: 'Contact Us',
        company: 'Company',
        aboutUs: 'About Us',
        news: 'News',
        research: 'Research',
        academicCoop: 'Academic Coop',
        careers: 'Careers',
        wechat: 'WeChat Subscription',
    },
    zh: {
        resource: '资源',
        roadmap: {
            name: '路线图',
            link: '/zh/guide/versions/roadmap.html',
        },
        devGuide: {
            name: '开发者手册',
            link: '/zh/dev-guide/get_started/build_source_code.html',
        },
        blog: '博客',
        support: '支持',
        forum: '社区',
        contactUs: '联系我们',
        company: '公司',
        aboutUs: '关于我们',
        news: '新闻',
        research: '研究方向',
        academicCoop: '学术合作',
        careers: '加入我们',
        wechat: '扫码关注公众号',
    },
};

export const getFooter = (lang = 'en') => {
    return `
		<footer id="custom-footer">
			<div class="content">
				<div class="media-link">
					<div class="logo">
						<img src="/images/horizontal.svg" />
					</div>
					<div class="links">
						<a class="icon iconfont" href="https://github.com/openGemini" target="_blank">&#xe600;</a>
						<a class="icon iconfont" href="https://join.slack.com/t/opengemini/shared_invite/zt-206txnqpc-UhWMF7DGXT3PLi~dr2Yd1Q" target="_blank">&#xe71b;</a>
						<a class="icon iconfont" href="https://twitter.com/OpenGemini" target="_blank">&#xe882;</a>
					</div>
					<div class="links">
						<a class="icon iconfont" href="https://www.zhihu.com/people/xiang-yu-47-54-80" target="_blank">&#xe69a;</a>
						<a class="icon iconfont" href="https://space.bilibili.com/1560037308?spm_id_from=333.337.0.0" target="_blank">&#xe601;</a>
						<a href="https://www.modb.pro/u/522879" target="_blank">
							<div class="motianlun"></div>
						</a>
					</div>
				</div>
				<div class="link-text">
					<div class="title">${langMap[lang].resource}</div>
					<a href="${langMap[lang].roadmap.link}">${langMap[lang].roadmap.name}</a>
					<a href="${langMap[lang].devGuide.link}">${langMap[lang].devGuide.name}</a>
					<a href="http://www.opengemini.org/blog" target="_blank">${langMap[lang].blog}</a>
				</div>
				<div class="link-text">
					<div class="title">${langMap[lang].support}</div>
					<a href="http://www.opengemini.org/events" target="_blank">${langMap[lang].forum}</a>
					<a href="https://jinshuju.net/f/V8sdbq" target="_blank">${langMap[lang].contactUs}</a>
				</div>
				<div class="qr-code">
					<img src="/images/wechat-qrcode.jpg" class="wechat" />
					<div>${langMap[lang].wechat}</div>
				</div>
			</div>
		</footer>`;
};
