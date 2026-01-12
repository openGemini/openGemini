<template>
    <div class="member">
        <img :src="info.img" alt="" :style="{ objectPosition: info.imgPosition || 'top' }" />
        <p class="name">{{ lang === 'en' ? info.enName : info.name }}</p>
        <p class="title">{{ info.title }}</p>
        <div class="links">
            <a :href="`mailto:${info.email}`">
                <FontIcon
                    class="iconfont"
                    icon="icon-myouxiang"
                    size="24"
                    style="margin-right: 8px"
                />
            </a>
            <a :href="info.github" target="_blank">
                <FontIcon class="iconfont" icon="icon-mgithub" size="24" />
            </a>
        </div>
    </div>
</template>

<script lang="ts" setup>
defineProps<{
    info: {
        name: string;
        enName: string;
        title: string;
        img: string;
        email: string;
        github: string;
        imgPosition: 'top' | 'center' | 'bottom';
    };
    lang: 'zh' | 'en';
}>();
</script>

<style scoped>
.member {
    width: 100px;

    display: flex;
    flex-direction: column;
    align-items: center;
}
img {
    width: 100px;
    min-width: 100px;
    height: 100px;
    border-radius: 50%;
    object-fit: cover;
    pointer-events: none;
    /* object-position: center; */
}
p {
    margin: 0;
    padding: 0;
}
.links {
    margin-top: 8px;
}
</style>
