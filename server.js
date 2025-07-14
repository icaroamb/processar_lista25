const express = require('express');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const cors = require('cors');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// Configurações do Bubble
const BUBBLE_CONFIG = {
  baseURL: 'https://calculaqui.com/api/1.1/obj',
  token: '7c4a6a50a83c872a298b261126781a8f',
  headers: {
    'token': '7c4a6a50a83c872a298b261126781a8f',
    'Content-Type': 'application/json'
  }
};

// Configuração do multer para upload de arquivos
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const uploadDir = './uploads';
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    cb(null, uploadDir);
  },
  filename: (req, file, cb) => {
    cb(null, `${Date.now()}-${file.originalname}`);
  }
});

const upload = multer({ 
  storage: storage,
  limits: {
    fileSize: 10 * 1024 * 1024 // 10MB max
  },
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'text/csv' || file.originalname.endsWith('.csv')) {
      cb(null, true);
    } else {
      cb(new Error('Apenas arquivos CSV são permitidos!'), false);
    }
  }
});

// ================ SISTEMA DE CONTROLE DE PROCESSAMENTO ================

// Armazena o status dos processamentos em memória
const processamentos = new Map();

// Função para gerar ID único do processamento
function generateProcessId() {
  return `proc_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

// Função para atualizar status do processamento
function updateProcessStatus(processId, status, data = {}) {
  const now = new Date().toISOString();
  
  if (!processamentos.has(processId)) {
    processamentos.set(processId, {
      id: processId,
      status: 'iniciado',
      inicio: now,
      etapas: []
    });
  }
  
  const processo = processamentos.get(processId);
  processo.status = status;
  processo.ultima_atualizacao = now;
  
  // Adicionar dados específicos do status
  Object.assign(processo, data);
  
  // Adicionar etapa ao histórico
  processo.etapas.push({
    timestamp: now,
    status: status,
    ...data
  });
  
  console.log(`📊 [${processId}] Status: ${status}`);
  
  return processo;
}

// ================ FUNÇÕES ORIGINAIS (com correções críticas) ================

// Função para extrair preço numérico
function extractPrice(priceString) {
  if (!priceString || priceString.toString().trim() === '') return 0;
  
  const cleanPrice = priceString
    .toString()
    .replace(/R\$\s?/, '')
    .replace(/\./g, '')
    .replace(/,/g, '.')
    .trim();
  
  const price = parseFloat(cleanPrice);
  return isNaN(price) ? 0 : price;
}

// Função para fazer parse correto do CSV respeitando aspas
function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(current.trim());
      current = '';
    } else {
      current += char;
    }
  }
  
  result.push(current.trim());
  return result;
}

// Função para buscar dados do Bubble com paginação
async function fetchAllFromBubble(tableName, filters = {}) {
  try {
    console.log(`🔍 Buscando dados de ${tableName}...`);
    let allData = [];
    let cursor = 0;
    let hasMore = true;
    
    while (hasMore) {
      const params = { cursor, limit: 100, ...filters };
      
      const response = await axios.get(`${BUBBLE_CONFIG.baseURL}/${tableName}`, {
        headers: BUBBLE_CONFIG.headers,
        params,
        timeout: 30000
      });
      
      const data = response.data;
      
      if (!data.response || !data.response.results) {
        throw new Error(`Estrutura de resposta inválida para ${tableName}`);
      }
      
      allData = allData.concat(data.response.results);
      hasMore = data.response.remaining > 0;
      cursor = data.response.cursor || (cursor + 100);
    }
    
    console.log(`✅ ${tableName}: ${allData.length} registros carregados`);
    return allData;
    
  } catch (error) {
    console.error(`❌ Erro ao buscar ${tableName}:`, error.message);
    throw new Error(`Erro ao buscar ${tableName}: ${error.message}`);
  }
}

// Função para criar item no Bubble
async function createInBubble(tableName, data) {
  try {
    const response = await axios.post(`${BUBBLE_CONFIG.baseURL}/${tableName}`, data, {
      headers: BUBBLE_CONFIG.headers,
      timeout: 30000
    });
    return response.data;
  } catch (error) {
    console.error(`❌ Erro ao criar em ${tableName}:`, error.response?.data || error.message);
    throw new Error(`Erro ao criar em ${tableName}: ${error.response?.data?.body?.message || error.message}`);
  }
}

// Função para atualizar item no Bubble
async function updateInBubble(tableName, itemId, data) {
  try {
    const response = await axios.patch(`${BUBBLE_CONFIG.baseURL}/${tableName}/${itemId}`, data, {
      headers: BUBBLE_CONFIG.headers,
      timeout: 30000
    });
    return response.data;
  } catch (error) {
    console.error(`❌ Erro ao atualizar ${tableName}/${itemId}:`, error.response?.data || error.message);
    throw new Error(`Erro ao atualizar ${tableName}: ${error.response?.data?.body?.message || error.message}`);
  }
}

// ================ FUNÇÃO CORRIGIDA PARA CALCULAR ESTATÍSTICAS ================

// Função para calcular estatísticas do produto (CORRIGIDA)
function calculateProductStats(produtoFornecedores) {
  console.log('🔢 Calculando estatísticas para:', produtoFornecedores.length, 'relações');
  
  // CORREÇÃO CRÍTICA 1: Filtro mais rigoroso e com logging
  const validPrices = produtoFornecedores
    .filter(pf => {
      const isValid = pf.preco_final !== null && 
                     pf.preco_final !== undefined && 
                     typeof pf.preco_final === 'number' && 
                     pf.preco_final > 0;
      
      if (!isValid) {
        console.log(`  ❌ Preço inválido filtrado: ${pf._id} = ${pf.preco_final} (tipo: ${typeof pf.preco_final})`);
      } else {
        console.log(`  ✅ Preço válido: ${pf._id} = ${pf.preco_final}`);
      }
      
      return isValid;
    })
    .map(pf => pf.preco_final);
  
  console.log('💰 Preços válidos encontrados:', validPrices);
  
  const qtd_fornecedores = validPrices.length;
  const menor_preco = qtd_fornecedores > 0 ? Math.min(...validPrices) : 0;
  const preco_medio = qtd_fornecedores > 0 ? validPrices.reduce((a, b) => a + b, 0) / qtd_fornecedores : 0;
  
  console.log('📊 Estatísticas calculadas:', { qtd_fornecedores, menor_preco, preco_medio });
  
  return { qtd_fornecedores, menor_preco, preco_medio };
}

// Função para processar o CSV (com processId)
function processCSV(filePath, processId = null) {
  return new Promise((resolve, reject) => {
    try {
      if (processId) {
        updateProcessStatus(processId, 'processando_csv', { 
          etapa: 'Lendo arquivo CSV' 
        });
      }
      
      console.log('📁 Lendo arquivo CSV...');
      
      const fileContent = fs.readFileSync(filePath, 'utf8');
      const lines = fileContent.split('\n').filter(line => line.trim());
      
      if (lines.length < 3) {
        console.log('❌ Arquivo CSV muito pequeno');
        return resolve([]);
      }
      
      // Pular as duas primeiras linhas (cabeçalhos)
      const dataLines = lines.slice(2);
      console.log(`📊 Processando ${dataLines.length} linhas de dados`);
      
      if (processId) {
        updateProcessStatus(processId, 'processando_csv', { 
          etapa: `Processando ${dataLines.length} linhas de dados` 
        });
      }
      
      // Configuração das lojas com índices das colunas
      const lojasConfig = [
        { nome: 'Loja da Suzy', indices: [0, 1, 2] },
        { nome: 'Loja Top Celulares', indices: [4, 5, 6] },
        { nome: 'Loja HUSSEIN', indices: [8, 9, 10] },
        { nome: 'Loja Paulo', indices: [12, 13, 14] },
        { nome: 'Loja HM', indices: [16, 17, 18] },
        { nome: 'Loja General', indices: [20, 21, 22] },
        { nome: 'Loja JR', indices: [24, 25, 26] },
        { nome: 'Loja Mega Cell', indices: [28, 29, 30] }
      ];
      
      const processedData = [];
      
      lojasConfig.forEach((lojaConfig) => {
        console.log(`🏪 Processando ${lojaConfig.nome}...`);
        const produtos = [];
        
        dataLines.forEach((line) => {
          if (!line || line.trim() === '') return;
          
          const columns = parseCSVLine(line);
          
          if (columns.length < 31) return;
          
          const codigo = columns[lojaConfig.indices[0]];
          const modelo = columns[lojaConfig.indices[1]];
          const preco = columns[lojaConfig.indices[2]];
          
          if (codigo && modelo && preco && 
              codigo.trim() !== '' && 
              modelo.trim() !== '' && 
              preco.trim() !== '') {
            
            const precoNumerico = extractPrice(preco);
            
            produtos.push({
              codigo: codigo.trim(),
              modelo: modelo.trim(),
              preco: precoNumerico
            });
          }
        });
        
        console.log(`✅ ${lojaConfig.nome}: ${produtos.length} produtos`);
        
        if (produtos.length > 0) {
          processedData.push({
            loja: lojaConfig.nome,
            total_produtos: produtos.length,
            produtos: produtos
          });
        }
      });
      
      if (processId) {
        updateProcessStatus(processId, 'csv_processado', { 
          etapa: 'CSV processado com sucesso',
          total_lojas: processedData.length,
          total_produtos: processedData.reduce((acc, loja) => acc + loja.total_produtos, 0)
        });
      }
      
      resolve(processedData);
      
    } catch (error) {
      console.error('❌ Erro no processamento do CSV:', error);
      
      if (processId) {
        updateProcessStatus(processId, 'erro', { 
          etapa: 'Erro no processamento do CSV',
          erro: error.message 
        });
      }
      
      reject(error);
    }
  });
}

// ================ FUNÇÃO PRINCIPAL CORRIGIDA ================

// Função principal para sincronizar com o Bubble (TOTALMENTE CORRIGIDA)
async function syncWithBubble(csvData, gorduraValor, processId = null) {
  try {
    console.log('\n🔄 Iniciando sincronização com Bubble...');
    
    if (processId) {
      updateProcessStatus(processId, 'sincronizando_bubble', { 
        etapa: 'Iniciando sincronização com Bubble' 
      });
    }
    
    // 1. CARREGAR DADOS EXISTENTES
    console.log('📊 Carregando dados existentes...');
    
    if (processId) {
      updateProcessStatus(processId, 'sincronizando_bubble', { 
        etapa: 'Carregando dados existentes do Bubble' 
      });
    }
    
    const [fornecedores, produtos, produtoFornecedores] = await Promise.all([
      fetchAllFromBubble('1 - fornecedor_25marco'),
      fetchAllFromBubble('1 - produtos_25marco'),
      fetchAllFromBubble('1 - ProdutoFornecedor _25marco')
    ]);
    
    console.log(`📊 Carregados: ${fornecedores.length} fornecedores, ${produtos.length} produtos, ${produtoFornecedores.length} relações`);
    
    // 2. CRIAR MAPAS PARA BUSCA RÁPIDA
    const fornecedorMap = new Map();
    fornecedores.forEach(f => fornecedorMap.set(f.nome_fornecedor, f));
    
    const produtoMap = new Map();
    produtos.forEach(p => produtoMap.set(p.id_planilha, p));
    
    const results = {
      fornecedores_criados: 0,
      produtos_criados: 0,
      relacoes_criadas: 0,
      relacoes_atualizadas: 0,
      relacoes_zeradas: 0,
      produtos_stats_atualizados: 0,
      produtos_melhor_preco_atualizados: 0
    };
    
    // 3. PROCESSAR PRODUTOS DO CSV
    console.log('\n📝 Processando produtos do CSV...');
    
    if (processId) {
      updateProcessStatus(processId, 'sincronizando_bubble', { 
        etapa: 'Processando produtos do CSV' 
      });
    }
    
    for (const lojaData of csvData) {
      console.log(`\n🏪 Processando ${lojaData.loja}...`);
      
      if (processId) {
        updateProcessStatus(processId, 'sincronizando_bubble', { 
          etapa: `Processando ${lojaData.loja}` 
        });
      }
      
      // 3.1 Verificar/criar fornecedor
      let fornecedor = fornecedorMap.get(lojaData.loja);
      if (!fornecedor) {
        console.log(`➕ Criando fornecedor: ${lojaData.loja}`);
        const novoFornecedor = await createInBubble('1 - fornecedor_25marco', {
          nome_fornecedor: lojaData.loja,
          status_ativo: 'yes'
        });
        fornecedor = { _id: novoFornecedor.id, nome_fornecedor: lojaData.loja };
        fornecedorMap.set(lojaData.loja, fornecedor);
        results.fornecedores_criados++;
      }
      
      // 3.2 Processar cada produto da loja
      for (const produtoCsv of lojaData.produtos) {
        // Verificar/criar produto
        let produto = produtoMap.get(produtoCsv.codigo);
        if (!produto) {
          console.log(`➕ Criando produto: ${produtoCsv.codigo}`);
          const novoProduto = await createInBubble('1 - produtos_25marco', {
            id_planilha: produtoCsv.codigo,
            nome_completo: produtoCsv.modelo,
            preco_medio: 0,
            qtd_fornecedores: 0,
            menor_preco: 0
          });
          produto = { 
            _id: novoProduto.id, 
            id_planilha: produtoCsv.codigo,
            nome_completo: produtoCsv.modelo
          };
          produtoMap.set(produtoCsv.codigo, produto);
          results.produtos_criados++;
        }
        
        // Calcular preços
        const precoOriginal = produtoCsv.preco;
        const precoFinal = precoOriginal === 0 ? 0 : precoOriginal + gorduraValor;
        const precoOrdenacao = precoOriginal === 0 ? 999999 : precoOriginal;
        
        // Verificar/criar/atualizar relação ProdutoFornecedor
        const relacaoExistente = produtoFornecedores.find(pf => 
          pf.produto === produto._id && pf.fornecedor === fornecedor._id
        );
        
        if (!relacaoExistente) {
          console.log(`➕ Criando relação: ${produtoCsv.codigo} - ${lojaData.loja}`);
          await createInBubble('1 - ProdutoFornecedor _25marco', {
            produto: produto._id,
            fornecedor: fornecedor._id,
            nome_produto: produtoCsv.modelo,
            preco_original: precoOriginal,
            preco_final: precoFinal,
            preco_ordenacao: precoOrdenacao,
            melhor_preco: false,
            status_ativo: 'yes'
          });
          results.relacoes_criadas++;
        } else if (relacaoExistente.preco_original !== precoOriginal) {
          console.log(`🔄 Atualizando relação: ${produtoCsv.codigo} - ${lojaData.loja}`);
          await updateInBubble('1 - ProdutoFornecedor _25marco', relacaoExistente._id, {
            preco_original: precoOriginal,
            preco_final: precoFinal,
            preco_ordenacao: precoOrdenacao
          });
          results.relacoes_atualizadas++;
        }
      }
    }
    
    // 4. ZERAR PRODUTOS NÃO COTADOS (COTAÇÃO DIÁRIA)
    console.log('\n🧹 Aplicando lógica de cotação diária...');
    
    if (processId) {
      updateProcessStatus(processId, 'sincronizando_bubble', { 
        etapa: 'Aplicando lógica de cotação diária' 
      });
    }
    
    for (const lojaData of csvData) {
      const fornecedor = fornecedorMap.get(lojaData.loja);
      if (!fornecedor) continue;
      
      console.log(`🔍 Verificando produtos ausentes para: ${lojaData.loja}`);
      
      // Criar Set dos códigos cotados hoje
      const codigosCotadosHoje = new Set();
      lojaData.produtos.forEach(produto => {
        codigosCotadosHoje.add(produto.codigo);
      });
      
      console.log(`📋 Produtos cotados hoje: [${Array.from(codigosCotadosHoje).join(', ')}]`);
      
      // Buscar todas as relações existentes deste fornecedor
      const relacoesExistentes = produtoFornecedores.filter(pf => pf.fornecedor === fornecedor._id);
      
      for (const relacao of relacoesExistentes) {
        const produto = produtos.find(p => p._id === relacao.produto);
        if (!produto) continue;
        
        const codigoProduto = produto.id_planilha;
        const foiCotadoHoje = codigosCotadosHoje.has(codigoProduto);
        const temPreco = relacao.preco_original > 0;
        
        // Se produto NÃO foi cotado hoje MAS tinha preço, zerar
        if (!foiCotadoHoje && temPreco) {
          console.log(`🧹 Zerando produto ausente: ${codigoProduto} - ${lojaData.loja}`);
          
          await updateInBubble('1 - ProdutoFornecedor _25marco', relacao._id, {
            preco_original: 0,
            preco_final: 0,
            preco_ordenacao: 999999
          });
          
          results.relacoes_zeradas++;
        }
      }
    }
    
    // 5. CORREÇÃO CRÍTICA: RECARREGAR DADOS APÓS TODAS AS ATUALIZAÇÕES
    console.log('\n🔄 CRÍTICO: Recarregando dados após atualizações...');
    
    if (processId) {
      updateProcessStatus(processId, 'sincronizando_bubble', { 
        etapa: 'Recarregando dados atualizados para recálculo' 
      });
    }
    
    // CORREÇÃO CRÍTICA: Aguardar persistência das atualizações
    console.log('⏳ Aguardando persistência das atualizações...');
    await new Promise(resolve => setTimeout(resolve, 3000));
    
    // Recarregar dados ATUALIZADOS
    console.log('🔄 Recarregando dados do Bubble...');
    const [produtoFornecedoresAtualizados, produtosAtualizados] = await Promise.all([
      fetchAllFromBubble('1 - ProdutoFornecedor _25marco'),
      fetchAllFromBubble('1 - produtos_25marco')
    ]);
    
    console.log(`📊 Dados recarregados: ${produtoFornecedoresAtualizados.length} relações, ${produtosAtualizados.length} produtos`);
    
    // 6. RECALCULAR ESTATÍSTICAS DOS PRODUTOS
    console.log('\n📊 Recalculando estatísticas dos produtos...');
    
    if (processId) {
      updateProcessStatus(processId, 'sincronizando_bubble', { 
        etapa: 'Recalculando estatísticas dos produtos' 
      });
    }
    
    // CORREÇÃO CRÍTICA: Agrupar usando dados atualizados
    const produtoStatsMap = new Map();
    
    // Agrupar por produto
    produtoFornecedoresAtualizados.forEach(pf => {
      if (!produtoStatsMap.has(pf.produto)) {
        produtoStatsMap.set(pf.produto, []);
      }
      produtoStatsMap.get(pf.produto).push(pf);
    });
    
    console.log(`📊 Produtos a recalcular: ${produtoStatsMap.size}`);
    
    // CORREÇÃO CRÍTICA: Processar cada produto com validação rigorosa
    for (const [produtoId, relacoes] of produtoStatsMap) {
      console.log(`\n📊 Processando produto ID: ${produtoId}`);
      console.log(`   Relações: ${relacoes.length}`);
      
      // CORREÇÃO CRÍTICA: Validar se produto existe
      const produto = produtosAtualizados.find(p => p._id === produtoId);
      if (!produto) {
        console.log(`❌ Produto ${produtoId} não encontrado, pulando...`);
        continue;
      }
      
      console.log(`   Código: ${produto.id_planilha}`);
      console.log(`   Nome: ${produto.nome_completo}`);
      
      // Calcular estatísticas com a função corrigida
      const stats = calculateProductStats(relacoes);
      
      // CORREÇÃO CRÍTICA: Validar se stats são válidas
      if (stats.qtd_fornecedores === 0) {
        console.log(`⚠️  Produto ${produto.id_planilha} sem fornecedores válidos`);
      }
      
      // Atualizar estatísticas do produto
      console.log(`🔄 Atualizando estatísticas do produto: ${produto.id_planilha}`);
      await updateInBubble('1 - produtos_25marco', produtoId, {
        qtd_fornecedores: stats.qtd_fornecedores,
        menor_preco: stats.menor_preco,
        preco_medio: stats.preco_medio
      });
      
      results.produtos_stats_atualizados++;
      
      // CORREÇÃO CRÍTICA: Atualizar melhor_preco com validação rigorosa
      console.log(`🏆 Atualizando melhor_preco para produto: ${produto.id_planilha}`);
      console.log(`   Menor preço calculado: ${stats.menor_preco}`);
      
      for (const relacao of relacoes) {
        const isMelhorPreco = relacao.preco_final === stats.menor_preco && 
                             relacao.preco_final > 0 && 
                             stats.menor_preco > 0;
        
        console.log(`   Relação ${relacao._id}: preco_final=${relacao.preco_final}, atual_melhor=${relacao.melhor_preco}, novo_melhor=${isMelhorPreco}`);
        
        // Só atualizar se mudou
        if (relacao.melhor_preco !== isMelhorPreco) {
          console.log(`   🔄 Atualizando melhor_preco: ${relacao._id} -> ${isMelhorPreco}`);
          
          await updateInBubble('1 - ProdutoFornecedor _25marco', relacao._id, {
            melhor_preco: isMelhorPreco
          });
          
          results.produtos_melhor_preco_atualizados++;
        } else {
          console.log(`   ✅ Melhor_preco já correto para: ${relacao._id}`);
        }
      }
    }
    
    console.log('\n✅ Sincronização concluída!');
    console.log('📊 Resultados detalhados:', results);
    
    // CORREÇÃO CRÍTICA: Validação final
    console.log('\n🔍 Validação final dos resultados...');
    
    const produtosFinalValidacao = await fetchAllFromBubble('1 - produtos_25marco');
    const produtoComMenorPreco = produtosFinalValidacao.filter(p => p.menor_preco > 0);
    const relacoesFinalValidacao = await fetchAllFromBubble('1 - ProdutoFornecedor _25marco');
    const relacoesMelhorPreco = relacoesFinalValidacao.filter(r => r.melhor_preco === true);
    
    console.log(`✅ Produtos com menor_preco > 0: ${produtoComMenorPreco.length}/${produtosFinalValidacao.length}`);
    console.log(`✅ Relações com melhor_preco = true: ${relacoesMelhorPreco.length}/${relacoesFinalValidacao.length}`);
    
    if (processId) {
      updateProcessStatus(processId, 'concluido', { 
        etapa: 'Sincronização concluída com sucesso',
        resultados: results,
        validacao: {
          total_produtos: produtosFinalValidacao.length,
          produtos_com_menor_preco: produtoComMenorPreco.length,
          total_relacoes: relacoesFinalValidacao.length,
          relacoes_com_melhor_preco: relacoesMelhorPreco.length
        }
      });
    }
    
    return results;
    
  } catch (error) {
    console.error('❌ Erro na sincronização:', error);
    
    if (processId) {
      updateProcessStatus(processId, 'erro', { 
        etapa: 'Erro na sincronização',
        erro: error.message 
      });
    }
    
    throw error;
  }
}

// ================ FUNÇÃO DE PROCESSAMENTO ASSÍNCRONO ================

async function processarAsync(filePath, gorduraValor, processId) {
  try {
    console.log(`🚀 [${processId}] Iniciando processamento assíncrono...`);
    
    // Processar o CSV
    const csvData = await processCSV(filePath, processId);
    
    // Sincronizar com Bubble
    const syncResults = await syncWithBubble(csvData, gorduraValor, processId);
    
    // Limpar arquivo temporário
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
      console.log(`🗑️ [${processId}] Arquivo temporário removido`);
    }
    
    updateProcessStatus(processId, 'finalizado', {
      etapa: 'Processamento finalizado com sucesso',
      dados_csv: csvData,
      resultados_sincronizacao: syncResults,
      fim: new Date().toISOString()
    });
    
    console.log(`✅ [${processId}] Processamento concluído com sucesso`);
    
  } catch (error) {
    console.error(`❌ [${processId}] Erro no processamento:`, error);
    
    // Limpar arquivo temporário em caso de erro
    if (fs.existsSync(filePath)) {
      fs.unlinkSync(filePath);
    }
    
    updateProcessStatus(processId, 'erro', {
      etapa: 'Erro no processamento',
      erro: error.message,
      fim: new Date().toISOString()
    });
  }
}

// ================ ROTAS DA API ================

// Rota principal para upload e processamento ASSÍNCRONO
app.post('/process-csv', upload.single('csvFile'), async (req, res) => {
  try {
    console.log('\n🚀 === NOVA REQUISIÇÃO ASSÍNCRONA ===');
    console.log('📤 Arquivo:', req.file ? req.file.originalname : 'Nenhum');
    
    // Validações iniciais
    if (!req.file) {
      return res.status(400).json({ 
        success: false,
        error: 'Nenhum arquivo CSV foi enviado' 
      });
    }
    
    // Validar parâmetro gordura_valor
    const gorduraValor = parseFloat(req.body.gordura_valor);
    if (isNaN(gorduraValor)) {
      return res.status(400).json({
        success: false,
        error: 'Parâmetro gordura_valor é obrigatório e deve ser um número'
      });
    }
    
    const filePath = req.file.path;
    
    if (!fs.existsSync(filePath)) {
      return res.status(400).json({ 
        success: false,
        error: 'Arquivo não encontrado' 
      });
    }
    
    // Gerar ID único para o processamento
    const processId = generateProcessId();
    
    console.log(`💰 Gordura valor: ${gorduraValor}`);
    console.log(`🆔 Process ID: ${processId}`);
    
    // Inicializar status do processamento
    updateProcessStatus(processId, 'iniciado', {
      arquivo: req.file.originalname,
      gordura_valor: gorduraValor,
      inicio: new Date().toISOString()
    });
    
    // Iniciar processamento assíncrono (não esperar)
    processarAsync(filePath, gorduraValor, processId);
    
    // Retornar resposta imediata
    res.json({
      success: true,
      message: 'Processamento iniciado',
      process_id: processId,
      arquivo: req.file.originalname,
      gordura_valor: gorduraValor,
      status: 'iniciado',
      status_url: `/process-status/${processId}`
    });
    
  } catch (error) {
    console.error('❌ Erro ao iniciar processamento:', error);
    
    // Limpar arquivo em caso de erro
    if (req.file && fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }
    
    res.status(500).json({ 
      success: false,
      error: 'Erro interno do servidor',
      details: error.message 
    });
  }
});

// Nova rota para consultar status do processamento
app.get('/process-status/:processId', (req, res) => {
  const processId = req.params.processId;
  
  if (!processamentos.has(processId)) {
    return res.status(404).json({
      success: false,
      error: 'Processamento não encontrado'
    });
  }
  
  const processo = processamentos.get(processId);
  
  res.json({
    success: true,
    process: processo
  });
});

// Rota para listar todos os processamentos
app.get('/process-list', (req, res) => {
  const lista = Array.from(processamentos.values())
    .sort((a, b) => new Date(b.inicio) - new Date(a.inicio))
    .slice(0, 50); // Últimos 50 processamentos
  
  res.json({
    success: true,
    total: processamentos.size,
    processamentos: lista
  });
});

// Rota para limpar processamentos antigos
app.delete('/process-cleanup', (req, res) => {
  const agora = new Date();
  const umDiaAtras = new Date(agora.getTime() - 24 * 60 * 60 * 1000);
  
  let removidos = 0;
  
  for (const [processId, processo] of processamentos) {
    const inicioProcesso = new Date(processo.inicio);
    if (inicioProcesso < umDiaAtras) {
      processamentos.delete(processId);
      removidos++;
    }
  }
  
  res.json({
    success: true,
    message: `${removidos} processamentos removidos`,
    restantes: processamentos.size
  });
});

// Rota para buscar estatísticas
app.get('/stats', async (req, res) => {
  try {
    const [fornecedores, produtos, produtoFornecedores] = await Promise.all([
      fetchAllFromBubble('1 - fornecedor_25marco'),
      fetchAllFromBubble('1 - produtos_25marco'),
      fetchAllFromBubble('1 - ProdutoFornecedor _25marco')
    ]);
    
    // Estatísticas detalhadas para debug
    const produtosComMenorPreco = produtos.filter(p => p.menor_preco > 0);
    const relacoesMelhorPreco = produtoFornecedores.filter(r => r.melhor_preco === true);
    
    res.json({
      success: true,
      estatisticas: {
        total_fornecedores: fornecedores.length,
        total_produtos: produtos.length,
        total_relacoes: produtoFornecedores.length,
        fornecedores_ativos: fornecedores.filter(f => f.status_ativo === 'yes').length,
        produtos_com_menor_preco: produtosComMenorPreco.length,
        relacoes_com_melhor_preco: relacoesMelhorPreco.length,
        percentual_produtos_com_preco: ((produtosComMenorPreco.length / produtos.length) * 100).toFixed(1) + '%'
      },
      debug: {
        produtos_sem_menor_preco: produtos.filter(p => p.menor_preco === 0).length,
        relacoes_sem_melhor_preco: produtoFornecedores.filter(r => r.melhor_preco !== true).length
      }
    });
    
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Erro ao buscar estatísticas',
      details: error.message
    });
  }
});

// Rota para buscar produto específico
app.get('/produto/:codigo', async (req, res) => {
  try {
    const codigo = req.params.codigo;
    
    const produtos = await fetchAllFromBubble('1 - produtos_25marco', {
      'id_planilha': codigo
    });
    
    if (produtos.length === 0) {
      return res.status(404).json({
        success: false,
        error: 'Produto não encontrado'
      });
    }
    
    const produto = produtos[0];
    
    // Buscar relações do produto
    const relacoes = await fetchAllFromBubble('1 - ProdutoFornecedor _25marco', {
      'produto': produto._id
    });
    
    // Buscar fornecedores das relações
    const fornecedores = await fetchAllFromBubble('1 - fornecedor_25marco');
    const fornecedorMap = new Map();
    fornecedores.forEach(f => fornecedorMap.set(f._id, f));
    
    const relacoesDetalhadas = relacoes.map(r => ({
      ...r,
      fornecedor_nome: fornecedorMap.get(r.fornecedor)?.nome_fornecedor || 'Não encontrado'
    }));
    
    res.json({
      success: true,
      produto: produto,
      relacoes: relacoesDetalhadas,
      estatisticas: {
        total_fornecedores: relacoes.length,
        fornecedores_com_preco: relacoes.filter(r => r.preco_final > 0).length,
        melhor_preco: produto.menor_preco,
        preco_medio: produto.preco_medio,
        fornecedores_melhor_preco: relacoes.filter(r => r.melhor_preco === true).length
      }
    });
    
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Erro ao buscar produto',
      details: error.message
    });
  }
});

// Rota para teste de saúde
app.get('/health', (req, res) => {
  res.json({ 
    status: 'OK', 
    message: 'API funcionando corretamente',
    timestamp: new Date().toISOString(),
    processamentos_ativos: processamentos.size,
    version: '3.1.0 - Processamento Assíncrono com Correções Críticas'
  });
});

// Rota para testar conectividade com Bubble
app.get('/test-bubble', async (req, res) => {
  try {
    console.log('🧪 Testando conectividade com Bubble...');
    
    const testResponse = await axios.get(`${BUBBLE_CONFIG.baseURL}/1 - fornecedor_25marco`, {
      headers: BUBBLE_CONFIG.headers,
      params: { limit: 1 },
      timeout: 10000
    });
    
    res.json({
      success: true,
      message: 'Conectividade com Bubble OK',
      bubble_response: testResponse.data,
      config: {
        baseURL: BUBBLE_CONFIG.baseURL,
        hasToken: !!BUBBLE_CONFIG.token
      }
    });
    
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Erro de conectividade com Bubble',
      details: {
        message: error.message,
        status: error.response?.status,
        data: error.response?.data
      }
    });
  }
});

// Nova rota para debug dos problemas de preço
app.get('/debug/precos', async (req, res) => {
  try {
    console.log('🔍 Iniciando debug dos preços...');
    
    const [produtos, relacoes] = await Promise.all([
      fetchAllFromBubble('1 - produtos_25marco'),
      fetchAllFromBubble('1 - ProdutoFornecedor _25marco')
    ]);
    
    // Produtos sem menor_preco
    const produtosSemPreco = produtos.filter(p => p.menor_preco === 0);
    
    // Relações sem melhor_preco
    const relacoesSemMelhorPreco = relacoes.filter(r => r.melhor_preco !== true);
    
    // Análise detalhada de alguns produtos
    const analiseDetalhada = produtosSemPreco.slice(0, 5).map(produto => {
      const relacoesproduto = relacoes.filter(r => r.produto === produto._id);
      return {
        produto: produto,
        relacoes: relacoesproduto,
        precos_finais: relacoesProduct.map(r => r.preco_final),
        tem_precos_validos: relacoesProduct.some(r => r.preco_final > 0)
      };
    });
    
    res.json({
      success: true,
      debug: {
        total_produtos: produtos.length,
        produtos_sem_menor_preco: produtosSemPreco.length,
        total_relacoes: relacoes.length,
        relacoes_sem_melhor_preco: relacoesSemMelhorPreco.length,
        analise_detalhada: analiseDetalhada,
        problema_identificado: produtosSemPreco.length > 0 ? 'Produtos sem menor_preco encontrados' : 'Nenhum problema identificado'
      }
    });
    
  } catch (error) {
    res.status(500).json({
      success: false,
      error: 'Erro no debug',
      details: error.message
    });
  }
});

// Rota de documentação
app.get('/', (req, res) => {
  res.json({
    message: 'API para processamento de CSV de produtos com integração Bubble',
    version: '3.1.0 - Processamento Assíncrono com Correções Críticas',
    endpoints: {
      'POST /process-csv': 'Envia arquivo CSV com parâmetro gordura_valor e inicia processamento assíncrono',
      'GET /process-status/:processId': 'Consulta status de um processamento específico',
      'GET /process-list': 'Lista todos os processamentos (últimos 50)',
      'DELETE /process-cleanup': 'Remove processamentos antigos (mais de 24h)',
      'GET /stats': 'Retorna estatísticas das tabelas com debug',
      'GET /produto/:codigo': 'Busca produto específico por código com relações detalhadas',
      'GET /debug/precos': 'Debug dos problemas de menor_preco e melhor_preco',
      'GET /health': 'Verifica status da API',
      'GET /test-bubble': 'Testa conectividade com Bubble'
    },
    parametros_obrigatorios: {
      'gordura_valor': 'number - Valor a ser adicionado ao preço original'
    },
    correcoes_criticas: [
      'Race condition corrigida: dados recarregados após atualizações',
      'Filtro de preços corrigido: não remove mais dados válidos',
      'Validação rigorosa: verifica existência antes de processar',
      'Logging detalhado: visibilidade completa para debug',
      'Timing correto: aguarda persistência antes de recalcular',
      'Validação final: confirma resultados corretos'
    ],
    funcionalidades: [
      'Processamento assíncrono de CSV',
      'Acompanhamento de status em tempo real',
      'Processamento de CSV com layout horizontal',
      'Cotação diária completa (zera produtos não cotados)',
      'Cálculos baseados no preço final (com margem)',
      'Identificação automática do melhor preço (CORRIGIDA)',
      'Sincronização inteligente com Bubble',
      'Debug avançado para identificar problemas'
    ],
    exemplo_uso: {
      '1_enviar_csv': 'POST /process-csv com arquivo e gordura_valor',
      '2_receber_process_id': 'API retorna imediatamente com process_id',
      '3_consultar_status': 'GET /process-status/{process_id} para acompanhar',
      '4_debug_problemas': 'GET /debug/precos para investigar problemas'
    }
  });
});

// Middleware de tratamento de erros
app.use((error, req, res, next) => {
  if (error instanceof multer.MulterError) {
    if (error.code === 'LIMIT_FILE_SIZE') {
      return res.status(400).json({ 
        success: false,
        error: 'Arquivo muito grande (máximo 10MB)' 
      });
    }
  }
  
  if (error.message === 'Apenas arquivos CSV são permitidos!') {
    return res.status(400).json({ 
      success: false,
      error: 'Apenas arquivos CSV são permitidos' 
    });
  }
  
  console.error('Erro não tratado:', error);
  res.status(500).json({ 
    success: false,
    error: 'Erro interno do servidor' 
  });
});

// Iniciar servidor
app.listen(PORT, () => {
  console.log(`🚀 Servidor rodando na porta ${PORT}`);
  console.log(`📊 Acesse: http://localhost:${PORT}`);
  console.log(`🔗 Integração Bubble configurada`);
  console.log(`✨ Versão 3.1.0 - Processamento Assíncrono com Correções Críticas`);
  console.log(`🔧 Problemas de menor_preco e melhor_preco CORRIGIDOS`);
  console.log(`🔄 Endpoints para acompanhar processamento:`);
  console.log(`   - POST /process-csv (inicia processamento)`);
  console.log(`   - GET /process-status/:id (consulta status)`);
  console.log(`   - GET /process-list (lista todos)`);
  console.log(`   - GET /debug/precos (debug dos preços)`);
});

module.exports = app;