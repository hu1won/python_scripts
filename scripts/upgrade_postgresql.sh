#!/bin/bash

# PostgreSQL 업그레이드 스크립트
# macOS Homebrew용

echo "🔄 PostgreSQL 업그레이드를 시작합니다..."

# 현재 버전 확인
echo "📋 현재 pg_dump 버전:"
pg_dump --version

# PostgreSQL 16 설치
echo "📦 PostgreSQL 16 설치 중..."
brew install postgresql@16

# PATH에 추가
echo "🔧 PATH 설정 중..."
echo 'export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"' >> ~/.zshrc

# 현재 세션에 적용
export PATH="/opt/homebrew/opt/postgresql@16/bin:$PATH"

# 새 버전 확인
echo "✅ 업그레이드 완료!"
echo "📋 새로운 pg_dump 버전:"
pg_dump --version

echo ""
echo "⚠️  주의: 터미널을 재시작하거나 다음 명령어를 실행하세요:"
echo "source ~/.zshrc"
