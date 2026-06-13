const fs = require('fs');
const path = require('path');
const { PDFDocument, StandardFonts, rgb } = require('pdf-lib');

const sourcePath = process.env.SOURCE_GUIDE_PDF
  || 'C:\\Users\\Yogesh.Dahale\\Downloads\\Coaching_Enrollment_User_Guide (2).pdf';
const outputPath = path.join(__dirname, '..', 'assets', 'coaching-enrollment-user-guide-v2.pdf');

const inches = (value) => value * 72;
const money = (amount) => `$${amount.toLocaleString('en-US')}`;

function drawTextBox(page, { x, y, w, h, fill, border, title, body, fonts }) {
  page.drawRectangle({
    x,
    y,
    width: w,
    height: h,
    color: fill,
    borderColor: border,
    borderWidth: 1
  });
  page.drawText(title, {
    x: x + 14,
    y: y + h - 24,
    size: 12,
    font: fonts.bold,
    color: rgb(0.04, 0.15, 0.27)
  });
  page.drawText(body, {
    x: x + 14,
    y: y + h - 44,
    size: 9,
    font: fonts.regular,
    color: rgb(0.12, 0.16, 0.21),
    lineHeight: 13,
    maxWidth: w - 28
  });
}

async function main() {
  if (!fs.existsSync(sourcePath)) {
    throw new Error(`Source guide PDF not found: ${sourcePath}`);
  }

  const out = await PDFDocument.create();
  const regular = await out.embedFont(StandardFonts.Helvetica);
  const bold = await out.embedFont(StandardFonts.HelveticaBold);
  const fonts = { regular, bold };

  const cover = out.addPage([inches(8.5), inches(11)]);
  const { width, height } = cover.getSize();
  const navy = rgb(0.04, 0.15, 0.27);
  const blue = rgb(0.09, 0.41, 0.67);
  const teal = rgb(0.11, 0.60, 0.55);
  const lightBlue = rgb(0.92, 0.96, 0.99);
  const lightTeal = rgb(0.91, 0.97, 0.96);
  const gold = rgb(0.96, 0.73, 0.25);
  const gray = rgb(0.33, 0.38, 0.44);

  cover.drawRectangle({ x: 0, y: height - 120, width, height: 120, color: navy });
  cover.drawText('MAJOR LEAGUE CRICKET ACADEMY PHILADELPHIA', {
    x: 54,
    y: height - 48,
    size: 10,
    font: bold,
    color: rgb(0.83, 0.91, 1)
  });
  cover.drawText('Coaching Enrollment Guide', {
    x: 54,
    y: height - 86,
    size: 27,
    font: bold,
    color: rgb(1, 1, 1)
  });
  cover.drawRectangle({ x: 410, y: height - 95, width: 130, height: 30, color: gold });
  cover.drawText('PARENT QUICK START', {
    x: 424,
    y: height - 84,
    size: 9,
    font: bold,
    color: navy
  });

  cover.drawText('Use this guide after receiving your coach recommendation email. The screenshot walkthrough starts on the next page.', {
    x: 54,
    y: height - 160,
    size: 12,
    font: regular,
    color: gray,
    maxWidth: 500,
    lineHeight: 16
  });

  drawTextBox(cover, {
    x: 54,
    y: height - 265,
    w: 155,
    h: 72,
    fill: lightTeal,
    border: rgb(0.75, 0.90, 0.87),
    title: '1. Create CricClubs ID',
    body: 'Register your child first at cricclubs.com/StarSportsUSYouthCricketLeague.',
    fonts
  });
  drawTextBox(cover, {
    x: 228,
    y: height - 265,
    w: 155,
    h: 72,
    fill: lightBlue,
    border: rgb(0.78, 0.88, 0.96),
    title: '2. Complete Form',
    body: 'Login using parent email and code. Enter parent, child, and CricClubs details.',
    fonts
  });
  drawTextBox(cover, {
    x: 402,
    y: height - 265,
    w: 155,
    h: 72,
    fill: rgb(1, 0.98, 0.90),
    border: rgb(0.94, 0.84, 0.57),
    title: '3. Checkout',
    body: 'Choose the recommended program and batch, then complete checkout to set up auto pay.',
    fonts
  });

  cover.drawText('Fee Options', { x: 54, y: height - 325, size: 17, font: bold, color: navy });
  const feeY = height - 362;
  const rows = [
    ['Program', 'Quarterly', 'Semi-Annual', 'Annual'],
    ['Beginner', money(450), money(900), money(1700)],
    ['Intermediate', money(575), money(1150), money(2200)]
  ];
  const colX = [54, 185, 305, 435];
  const colW = [130, 120, 130, 122];
  rows.forEach((row, r) => {
    const y = feeY - r * 32;
    cover.drawRectangle({
      x: 54,
      y: y - 10,
      width: 503,
      height: 30,
      color: r === 0 ? blue : rgb(1, 1, 1),
      borderColor: rgb(0.84, 0.88, 0.92),
      borderWidth: 1
    });
    row.forEach((cell, c) => {
      cover.drawText(cell, {
        x: colX[c] + 8,
        y,
        size: r === 0 ? 9.5 : 10.5,
        font: r === 0 || c === 0 ? bold : regular,
        color: r === 0 ? rgb(1, 1, 1) : navy,
        maxWidth: colW[c] - 16
      });
    });
  });

  cover.drawText('What happens after checkout?', { x: 54, y: height - 500, size: 17, font: bold, color: navy });
  cover.drawText('Your child is officially enrolled, the coaching team confirms the assigned batch, and you will be added to the WhatsApp group for coaching communication.', {
    x: 54,
    y: height - 528,
    size: 11,
    font: regular,
    color: gray,
    maxWidth: 500,
    lineHeight: 16
  });

  cover.drawRectangle({ x: 54, y: 78, width: 503, height: 62, color: lightBlue, borderColor: rgb(0.78, 0.88, 0.96), borderWidth: 1 });
  cover.drawText('Need help?', { x: 72, y: 116, size: 12, font: bold, color: navy });
  cover.drawText('Reply to the MLCA coaching email if you need help creating the CricClubs ID, choosing a batch, or completing payment.', {
    x: 72,
    y: 94,
    size: 9.5,
    font: regular,
    color: gray,
    maxWidth: 468,
    lineHeight: 13
  });

  const source = await PDFDocument.load(fs.readFileSync(sourcePath));
  const copiedPages = await out.copyPages(source, source.getPageIndices());
  copiedPages.forEach((page) => out.addPage(page));

  fs.mkdirSync(path.dirname(outputPath), { recursive: true });
  fs.writeFileSync(outputPath, await out.save());
  console.log(`Created ${outputPath}`);
  console.log(`Included ${copiedPages.length} original screenshot page(s) from ${sourcePath}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
