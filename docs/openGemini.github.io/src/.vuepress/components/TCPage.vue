<template>
    <div class="tc-page">
        <h2>{{ lang === 'en' ? 'Special Interest Groups(SIG)' : '专项兴趣小组(SIG)' }}</h2>
        <div class="card">
            <div v-for="group in SIG" class="group" :key="group.groupName">
                <p class="group-name">{{ group.groupName }}</p>
                <div class="flex-center">
                    <FontIcon class="iconfont" icon="icon-mhome" size="24" />
                    <a :href="group.github" target="_blank">{{
                        lang === 'en' ? 'Go to Github home page' : '前往Github仓库主页'
                    }}</a>
                </div>
                <div class="flex-center">
                    <FontIcon class="iconfont" icon="icon-mchengyuan" size="24" />
                    <span>{{ lang === 'en' ? 'Core Members' : '核心成员' }}</span>
                </div>
                <div class="group-info">
                    <Member
                        v-for="item in group.member"
                        :lang="lang"
                        :key="item.name"
                        :info="item"
                    />
                </div>
            </div>
        </div>
    </div>
</template>

<script lang="ts" setup>
import { onMounted, onUnmounted } from 'vue';
import Member from './Member.vue';
import { SIG } from '../data/TCData';

defineProps<{
    lang: 'zh' | 'en';
}>();
// onMounted(() => {
// 	const page = document.querySelector('.page') as HTMLElement
// 	page.style.backgroundColor = 'rgb(245,246,248)'
// })
// onUnmounted(() => {
// 	const page = document.querySelector('.page') as HTMLElement
// 	page.style.backgroundColor = '#fff'
// })
</script>

<style>
html[data-theme='light'] {
    --tc-bg-color: rgb(245, 246, 248);
}
html[data-theme='dark'] {
    --tc-bg-color: #282828;
}
</style>
<style scoped>
h2 {
    border: 0;
    text-align: center;
}
.card {
    display: flex;
    flex-direction: column;
    background: var(--tc-bg-color);
    box-shadow: 0 1px 5px rgba(45, 47, 51, 0.1);
    padding: 40px 64px;
}

.group-name {
    font-size: 1.2rem;
    font-weight: bold;
    margin-bottom: 12px;
}
.group-info {
    margin: 16px 0;
    font-size: 0.8rem;
    display: flex;
    flex-wrap: wrap;
    gap: 16px 56px;
}
.card .flex-center a {
    color: #7d32ea;
    text-decoration: none;
}
.flex-center {
    display: flex;
    align-items: center;
    font-size: 0.8rem;
    margin-bottom: 12px;
}
.flex-center .iconfont {
    margin-right: 12px;
}
</style>
